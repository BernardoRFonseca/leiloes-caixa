"""
Processa CSVs locais da Caixa (manualmente baixados do site).

Workflow:
  1. Faz download manual dos CSVs em https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp
  2. Coloca os ficheiros em uploads/ (ex: uploads/Lista_imoveis_BA.csv, uploads/Lista_imoveis_SP.csv, ...)
  3. Faz commit no GitHub (ou corre localmente)
  4. Este script processa todos, normaliza, e gera data/imoveis.parquet

Uso direto: python process_local.py
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

UPLOADS_DIR = Path(__file__).parent / "uploads"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CSV_ENCODING = "latin-1"
CSV_SEPARATOR = ";"
# 1ª linha: título "Lista de Imóveis da Caixa..."
# 2ª linha: vazia/separadora
# 3ª linha: cabeçalho real → header=0 com skiprows=2
CSV_SKIP_ROWS = 2

DETAIL_URL = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel={id}"
PHOTO_URL = "https://venda-imoveis.caixa.gov.br/fotos/F{id}21.jpg"

RENAME_MAP = {
    "N° do imóvel": "id_imovel",
    "Nº do imóvel": "id_imovel",
    "UF": "uf",
    "Cidade": "cidade",
    "Bairro": "bairro",
    "Endereço": "endereco",
    "Preço": "preco_venda",
    "Valor de avaliação": "valor_avaliacao",
    "Desconto": "desconto_pct",
    "Financiamento": "aceita_financiamento",
    "Descrição": "descricao",
    "Modalidade de venda": "modalidade",
    "Link de acesso": "link",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("process_local")


def _clean_money(value):
    """Converte valores monetários PT-BR ou US para float."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None
    # Se tem vírgula, é formato BR: ponto = milhar, vírgula = decimal
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    # Senão, ponto já é decimal (formato US)
    try:
        return float(s)
    except ValueError:
        return None


def parse_csv(path: Path) -> pd.DataFrame:
    log.info("→ A processar %s …", path.name)
    try:
        df = pd.read_csv(
            path,
            encoding=CSV_ENCODING,
            sep=CSV_SEPARATOR,
            skiprows=CSV_SKIP_ROWS,
            header=0,
            dtype=str,
            on_bad_lines="skip",
        )
    except Exception as e:
        log.error("   Erro a ler %s: %s", path.name, e)
        return pd.DataFrame()

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=RENAME_MAP)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
    df = df.dropna(how="all")

    if "id_imovel" not in df.columns:
        log.warning("   ⚠ %s: estrutura inválida, ignorado", path.name)
        return pd.DataFrame()

    log.info("   ✓ %s: %d imóveis", path.name, len(df))
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df["preco_venda"] = df["preco_venda"].apply(_clean_money)
    df["valor_avaliacao"] = df["valor_avaliacao"].apply(_clean_money)
    df["desconto_pct"] = df["desconto_pct"].apply(_clean_money)

    df["url_detalhe"] = df["id_imovel"].apply(
        lambda x: DETAIL_URL.format(id=x) if pd.notna(x) else None
    )
    df["url_foto"] = df["id_imovel"].apply(
        lambda x: PHOTO_URL.format(id=x) if pd.notna(x) else None
    )

    def _tipo(desc):
        if not isinstance(desc, str):
            return "Outro"
        d = desc.lower()
        if "apartamento" in d or "apto" in d:
            return "Apartamento"
        if "casa" in d:
            return "Casa"
        if "terreno" in d or "lote" in d or "gleba" in d:
            return "Terreno"
        if "comercial" in d or "loja" in d or "galp" in d:
            return "Comercial"
        if "rural" in d or "fazenda" in d or "sítio" in d or "sitio" in d:
            return "Rural"
        return "Outro"

    df["tipo_imovel"] = df["descricao"].apply(_tipo)

    def _quartos(desc):
        if not isinstance(desc, str):
            return None
        m = re.search(r"(\d+)\s*(?:quartos?|qtos?|qts?|qto|dorm)", desc.lower())
        return int(m.group(1)) if m else None

    df["quartos"] = df["descricao"].apply(_quartos)

    def _area(desc):
        if not isinstance(desc, str):
            return None
        m = re.search(r"([\d.]+)\s*de\s*[áa]rea\s*total", desc.lower())
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
        return None

    df["area_m2"] = df["descricao"].apply(_area)

    cols = [
        "id_imovel", "uf", "cidade", "bairro", "endereco",
        "tipo_imovel", "quartos", "area_m2",
        "preco_venda", "valor_avaliacao", "desconto_pct",
        "modalidade", "aceita_financiamento", "descricao",
        "url_detalhe", "url_foto", "link",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols + [c for c in df.columns if c not in cols]]
    return df


def main():
    csvs = sorted(UPLOADS_DIR.glob("Lista_imoveis_*.csv"))
    if not csvs:
        log.error("Nenhum CSV em %s/", UPLOADS_DIR.name)
        log.error("Faz download manual em https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp")
        log.error("e coloca os ficheiros nesta pasta.")
        return

    log.info("Encontrados %d CSVs", len(csvs))
    frames = []
    for p in csvs:
        df = parse_csv(p)
        if not df.empty:
            frames.append(df)

    if not frames:
        log.error("Nenhum CSV válido. A abortar.")
        return

    raw = pd.concat(frames, ignore_index=True)
    log.info("Total bruto: %d imóveis", len(raw))

    df = normalize(raw)
    log.info("Após normalização: %d imóveis", len(df))

    out_parquet = DATA_DIR / "imoveis.parquet"
    out_csv = DATA_DIR / "imoveis.csv"
    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)
    log.info("Gravado em %s (%d KB)", out_parquet, out_parquet.stat().st_size // 1024)

    log.info("─── Resumo ───────────────────────")
    for uf, n in df["uf"].value_counts().head(10).items():
        log.info("  %s: %d imóveis", uf, n)
    log.info("Tipos: %s", dict(df["tipo_imovel"].value_counts()))
    if df["preco_venda"].notna().any():
        log.info("Preço mediano: R$ %.0f", df["preco_venda"].median())
        log.info("Preço min/max: R$ %.0f / R$ %.0f", df["preco_venda"].min(), df["preco_venda"].max())
    if df["desconto_pct"].notna().any():
        log.info("Desconto mediano: %.1f%%", df["desconto_pct"].median())
        log.info("Desconto máximo: %.1f%%", df["desconto_pct"].max())


if __name__ == "__main__":
    main()
