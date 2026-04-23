"""
Scraper dos imóveis da Caixa Econômica Federal — versão corrigida.

O CSV publicado pela Caixa não tem cabeçalho de colunas — atribuímos nomes
explicitamente com base na ordem fixa das colunas observada nos ficheiros.
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

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
]

BASE_URL = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{uf}.csv"
DETAIL_URL = "https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel={id}"
PHOTO_URL = "https://venda-imoveis.caixa.gov.br/fotos/F{id}21.jpg"

CSV_ENCODING = "latin-1"
CSV_SEPARATOR = ";"
# As primeiras 5 linhas são apresentação ("Lista de Imóveis...", "Estado X", etc)
# A 6ª linha JÁ é dados — o ficheiro NÃO tem nomes de colunas.
CSV_SKIP_ROWS = 1

# Nomes que vamos atribuir às colunas, pela ordem fixa observada
COLUMN_NAMES = [
    "id_imovel",         # 0
    "uf",                # 1
    "cidade",            # 2
    "bairro",            # 3
    "endereco",          # 4
    "preco_venda",       # 5
    "valor_avaliacao",   # 6
    "desconto_pct",      # 7
    "aceita_financiamento",  # 8 ("Sim"/"Não")
    "descricao",         # 9
    "modalidade",        # 10
    "link",              # 11
]

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


def build_session() -> requests.Session:
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


def fetch_uf(session: requests.Session, uf: str) -> pd.DataFrame | None:
    url = BASE_URL.format(uf=uf)
    log.info("→ A descarregar %s …", uf)
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("   Falhou %s: %s", uf, e)
        return None

    if b"<html" in resp.content[:200].lower():
        log.warning("   Resposta HTML para %s — ignorado", uf)
        return None

    try:
        # header=None → não usar nenhuma linha como cabeçalho
        # names=COLUMN_NAMES → atribuir nomes explicitamente
df = pd.read_csv(
            io.BytesIO(resp.content),
            encoding=CSV_ENCODING,
            sep=CSV_SEPARATOR,
            skiprows=CSV_SKIP_ROWS,
            header=0,  # a próxima linha é o cabeçalho real
            dtype=str,
            on_bad_lines="skip",
        )
        # Renomear colunas: o CSV usa nomes em português, normalizamos
        df.columns = [c.strip() for c in df.columns]
        rename_map = {
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
        df = df.rename(columns=rename_map)
        # Limpa espaços nos valores string
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
    except Exception as e:
        log.warning("   Erro a fazer parse do CSV de %s: %s", uf, e)
        return None

    df = df.dropna(how="all")
    log.info("   ✓ %s: %d imóveis", uf, len(df))
    return df


def _clean_money(value) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    s = re.sub(r"[^\d,.\-]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _extract_cep(endereco) -> str | None:
    if not endereco or pd.isna(endereco):
        return None
    m = re.search(r"(\d{5})-?(\d{3})", str(endereco))
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Limpeza de tipos numéricos
    df["preco_venda"] = df["preco_venda"].apply(_clean_money)
    df["valor_avaliacao"] = df["valor_avaliacao"].apply(_clean_money)
    df["desconto_pct"] = df["desconto_pct"].apply(_clean_money)

    # CEP do endereço
    df["cep"] = df["endereco"].apply(_extract_cep)

    # URLs
    df["url_detalhe"] = df["id_imovel"].apply(
        lambda x: DETAIL_URL.format(id=x) if pd.notna(x) else None
    )
    df["url_foto"] = df["id_imovel"].apply(
        lambda x: PHOTO_URL.format(id=x) if pd.notna(x) else None
    )

    # Tipo de imóvel inferido da descrição
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

    # Quartos
    def _quartos(desc):
        if not isinstance(desc, str):
            return None
        m = re.search(r"(\d+)\s*(?:quartos?|qtos?|qts?|dorm)", desc.lower())
        return int(m.group(1)) if m else None

    df["quartos"] = df["descricao"].apply(_quartos)

    # Área total — tenta apanhar "X.XX de área total" (formato Caixa) ou "X m²"
    def _area(desc):
        if not isinstance(desc, str):
            return None
        # Formato Caixa: "Terreno, 100.50 de área total, 60.00 de área privativa"
        m = re.search(r"([\d.]+)\s*de\s*[áa]rea\s*total", desc.lower())
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
        # Formato com m²
        m = re.search(r"(\d+[.,]?\d*)\s*m[²2]", desc.lower())
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except ValueError:
                pass
        return None

    df["area_m2"] = df["descricao"].apply(_area)

    # Reordenação final
    cols = [
        "id_imovel", "uf", "cidade", "bairro", "endereco", "cep",
        "tipo_imovel", "quartos", "area_m2",
        "preco_venda", "valor_avaliacao", "desconto_pct",
        "modalidade", "aceita_financiamento", "descricao",
        "url_detalhe", "url_foto", "link",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols + [c for c in df.columns if c not in cols]]
    return df


def main():
    session = build_session()
    frames = []

    for uf in UFS:
        df = fetch_uf(session, uf)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(0.5)

    if not frames:
        log.error("Nenhum CSV foi descarregado. A abortar.")
        sys.exit(1)

    log.info("A consolidar %d datasets…", len(frames))
    raw = pd.concat(frames, ignore_index=True)
    log.info("Total bruto: %d linhas, %d colunas", len(raw), len(raw.columns))

    df = normalize(raw)
    log.info("Após normalização: %d imóveis", len(df))
    log.info("Colunas finais: %s", list(df.columns))

    out_parquet = DATA_DIR / "imoveis.parquet"
    out_csv = DATA_DIR / "imoveis.csv"
    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)
    log.info("Gravado em %s (%d KB)", out_parquet, out_parquet.stat().st_size // 1024)

    log.info("─── Resumo ───────────────────────")
    log.info("Top 10 UFs:")
    for uf, n in df["uf"].value_counts().head(10).items():
        log.info("  %s: %d imóveis", uf, n)
    log.info("Tipos: %s", dict(df["tipo_imovel"].value_counts()))
    if df["preco_venda"].notna().any():
        log.info("Preço mediano: R$ %.2f", df["preco_venda"].median())
    if df["desconto_pct"].notna().any():
        log.info("Desconto mediano: %.1f%%", df["desconto_pct"].median())
    log.info("CEPs extraídos: %d / %d", df["cep"].notna().sum(), len(df))


if __name__ == "__main__":
    main()
