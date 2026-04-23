"""
Scraper dos imóveis da Caixa Econômica Federal.

Descarrega o CSV oficial de cada estado (UF) a partir do endpoint público:
  https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{UF}.csv

Normaliza, junta tudo num único DataFrame e grava em data/imoveis.parquet.
"""

from __future__ import annotations

import io
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuração ----------------------------------------------------------------

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
]

BASE_URL = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{uf}.csv"
DETAIL_URL = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel={id}"
PHOTO_URL = "https://venda-imoveis.caixa.gov.br/fotos/F{id}21.jpg"

# A Caixa publica o CSV em latin-1, separado por ";", com cabeçalho nas primeiras linhas
CSV_ENCODING = "latin-1"
CSV_SEPARATOR = ";"
# As 5 primeiras linhas do ficheiro são header descritivo, a 6ª é a linha de colunas
CSV_SKIP_ROWS = 5

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# --- HTTP session com retries ----------------------------------------------------

def build_session() -> requests.Session:
    """Cria uma sessão HTTP com retries automáticos para tolerar instabilidade."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/csv,text/plain,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Referer": "https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp",
    })
    return session


# --- Download e parsing por UF ---------------------------------------------------

def fetch_uf(session: requests.Session, uf: str) -> pd.DataFrame | None:
    """Descarrega o CSV de uma UF e devolve um DataFrame."""
    url = BASE_URL.format(uf=uf)
    log.info("→ A descarregar %s …", uf)
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("   Falhou %s: %s", uf, e)
        return None

    # O servidor às vezes devolve HTML de erro com 200; sanity check
    if b"<html" in resp.content[:200].lower():
        log.warning("   Resposta HTML em vez de CSV para %s — ignorado", uf)
        return None

    try:
        df = pd.read_csv(
            io.BytesIO(resp.content),
            encoding=CSV_ENCODING,
            sep=CSV_SEPARATOR,
            skiprows=CSV_SKIP_ROWS,
            dtype=str,  # tudo como string, normalizamos depois
            on_bad_lines="skip",
        )
    except Exception as e:
        log.warning("   Erro a fazer parse do CSV de %s: %s", uf, e)
        return None

    # Os ficheiros têm uma linha vazia ou de "rodapé" no fim
    df = df.dropna(how="all")
    df["UF_origem"] = uf
    log.info("   ✓ %s: %d imóveis", uf, len(df))
    return df


# --- Normalização ----------------------------------------------------------------

def _clean_money(value: str | float | None) -> float | None:
    """Converte 'R$ 96.592,92' ou '96.592,92' em 96592.92."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    # remove tudo exceto dígitos, vírgula, ponto e sinal
    s = re.sub(r"[^\d,.\-]", "", s)
    # padrão pt-BR: ponto = milhar, vírgula = decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _clean_int(value: str | float | None) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = re.sub(r"[^\d\-]", "", str(value))
    try:
        return int(s) if s else None
    except ValueError:
        return None


def _extract_cep(endereco: str | None) -> str | None:
    """Extrai o CEP de uma string de endereço (formato 00000-000 ou 00000000)."""
    if not endereco:
        return None
    m = re.search(r"(\d{5})-?(\d{3})", str(endereco))
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza o DataFrame consolidado."""
    # Mapeamento de colunas conhecidas (o CSV usa nomes em maiúsculas com espaços)
    # Abaixo cobrimos várias variantes que aparecem nos ficheiros da Caixa
    rename_map = {
        "N° do imóvel": "id_imovel",
        "Nº do imóvel": "id_imovel",
        "N do imóvel": "id_imovel",
        "UF": "uf",
        "Cidade": "cidade",
        "Bairro": "bairro",
        "Endereço": "endereco",
        "Preço": "preco_venda",
        "Valor de avaliação": "valor_avaliacao",
        "Desconto": "desconto",
        "Descrição": "descricao",
        "Modalidade de venda": "modalidade",
        "Link de acesso": "link",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Garante colunas mínimas mesmo que o CSV mude
    for col in ["id_imovel", "uf", "cidade", "bairro", "endereco",
                "preco_venda", "valor_avaliacao", "desconto", "descricao",
                "modalidade", "link"]:
        if col not in df.columns:
            df[col] = None

    # Limpeza de tipos
    df["preco_venda"] = df["preco_venda"].apply(_clean_money)
    df["valor_avaliacao"] = df["valor_avaliacao"].apply(_clean_money)
    df["desconto_pct"] = df["desconto"].apply(_clean_money)

    # Extrai CEP do endereço
    df["cep"] = df["endereco"].apply(_extract_cep)

    # Calcula desconto se não vier
    mask_no_disc = df["desconto_pct"].isna() & df["valor_avaliacao"].gt(0).fillna(False)
    df.loc[mask_no_disc, "desconto_pct"] = (
        (1 - df.loc[mask_no_disc, "preco_venda"] / df.loc[mask_no_disc, "valor_avaliacao"]) * 100
    ).round(2)

    # URLs de detalhe e foto
    df["url_detalhe"] = df["id_imovel"].apply(
        lambda x: DETAIL_URL.format(id=x) if pd.notna(x) else None
    )
    df["url_foto"] = df["id_imovel"].apply(
        lambda x: PHOTO_URL.format(id=x) if pd.notna(x) else None
    )

    # Tipo de imóvel: tenta inferir da descrição
    def _tipo(desc):
        if not isinstance(desc, str):
            return "Outro"
        d = desc.lower()
        if "apartamento" in d or "apto" in d:
            return "Apartamento"
        if "casa" in d:
            return "Casa"
        if "terreno" in d or "lote" in d:
            return "Terreno"
        if "comercial" in d or "loja" in d or "sala" in d:
            return "Comercial"
        return "Outro"

    df["tipo_imovel"] = df["descricao"].apply(_tipo)

    # Quartos: extrai dígito antes de "quarto"/"qto"
    def _quartos(desc):
        if not isinstance(desc, str):
            return None
        m = re.search(r"(\d+)\s*(?:quartos?|qtos?|qts?|dorm)", desc.lower())
        return int(m.group(1)) if m else None

    df["quartos"] = df["descricao"].apply(_quartos)

    # Áreas: tenta extrair "XX,XX m²"
    def _area(desc):
        if not isinstance(desc, str):
            return None
        m = re.search(r"(\d+[.,]?\d*)\s*m[²2]", desc.lower())
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            return None

    df["area_m2"] = df["descricao"].apply(_area)

    # Reordena
    cols = [
        "id_imovel", "uf", "cidade", "bairro", "endereco", "cep",
        "tipo_imovel", "quartos", "area_m2",
        "preco_venda", "valor_avaliacao", "desconto_pct",
        "modalidade", "descricao",
        "url_detalhe", "url_foto", "link",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols + [c for c in df.columns if c not in cols]]

    return df


# --- Pipeline principal ----------------------------------------------------------

def main():
    session = build_session()
    frames = []

    for uf in UFS:
        df = fetch_uf(session, uf)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(0.5)  # cortesia para não martelar o servidor

    if not frames:
        log.error("Nenhum CSV foi descarregado com sucesso. A abortar.")
        sys.exit(1)

    log.info("A consolidar %d datasets…", len(frames))
    raw = pd.concat(frames, ignore_index=True)
    log.info("Total bruto: %d linhas, %d colunas", len(raw), len(raw.columns))
    log.info("Colunas detetadas: %s", list(raw.columns))

    df = normalize(raw)
    log.info("Após normalização: %d imóveis", len(df))

    # Persistência: parquet (eficiente) + CSV (debug)
    out_parquet = DATA_DIR / "imoveis.parquet"
    out_csv = DATA_DIR / "imoveis.csv"
    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)
    log.info("Gravado em %s (%d KB)", out_parquet, out_parquet.stat().st_size // 1024)

    # Stats rápidas
    log.info("─── Resumo ───────────────────────")
    log.info("Imóveis por UF (top 10):")
    for uf, n in df["uf"].value_counts().head(10).items():
        log.info("  %s: %d", uf, n)
    log.info("Preço mediano: R$ %.2f", df["preco_venda"].median())
    log.info("Desconto mediano: %.1f%%", df["desconto_pct"].median())


if __name__ == "__main__":
    main()
