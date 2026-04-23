"""
Scraper dos imóveis da Caixa Econômica Federal — versão final com
proteção anti-CAPTCHA do Radware Bot Manager.
"""

from __future__ import annotations

import io
import logging
import random
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
CSV_SKIP_ROWS = 1

# User-agents reais para rotação
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

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

# Colunas mínimas exigidas para considerar uma resposta válida
EXPECTED_COLUMNS = {"id_imovel", "uf", "cidade", "endereco", "preco_venda"}

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
        total=3,
        backoff_factor=2.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _is_captcha(content: bytes) -> bool:
    """Deteta se a resposta é uma página CAPTCHA do Radware."""
    head = content[:5000].lower()
    return (
        b"radware" in head
        or b"captcha" in head
        or b"<html" in head[:200]
        or b"<!doctype" in head[:200]
    )


def fetch_uf(session: requests.Session, uf: str, max_attempts: int = 3) -> pd.DataFrame | None:
    url = BASE_URL.format(uf=uf)

    for attempt in range(1, max_attempts + 1):
        log.info("→ A descarregar %s (tentativa %d/%d)…", uf, attempt, max_attempts)

        # Headers com user-agent rotativo
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Referer": "https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp",
            "Cache-Control": "no-cache",
        }

        try:
            resp = session.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.warning("   Falhou %s: %s", uf, e)
            time.sleep(random.uniform(5, 10))
            continue

        if _is_captcha(resp.content):
            wait = random.uniform(15, 30) * attempt
            log.warning("   ⚠ CAPTCHA detectado para %s. A esperar %.1fs antes de retry…", uf, wait)
            time.sleep(wait)
            continue

        # Sucesso aparente — tenta fazer parse
        try:
            df = pd.read_csv(
                io.BytesIO(resp.content),
                encoding=CSV_ENCODING,
                sep=CSV_SEPARATOR,
                skiprows=CSV_SKIP_ROWS,
                header=0,
                dtype=str,
                on_bad_lines="skip",
            )
        except Exception as e:
            log.warning("   Erro a fazer parse do CSV de %s: %s", uf, e)
            time.sleep(random.uniform(5, 10))
            continue

        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns=RENAME_MAP)

        # Validação: tem as colunas essenciais?
        missing = EXPECTED_COLUMNS - set(df.columns)
        if missing:
            log.warning("   ⚠ Colunas em falta em %s: %s. Resposta provavelmente inválida.", uf, missing)
            log.warning("   Colunas recebidas: %s", list(df.columns)[:5])
            time.sleep(random.uniform(10, 20))
            continue

        # Limpa espaços
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()

        df = df.dropna(how="all")
        log.info("   ✓ %s: %d imóveis", uf, len(df))
        return df

    log.error("   ✗ Desisti de %s após %d tentativas", uf, max_attempts)
    return None


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
    df["preco_venda"] = df["preco_venda"].apply(_clean_money)
    df["valor_avaliacao"] = df["valor_avaliacao"].apply(_clean_money)
    df["desconto_pct"] = df["desconto_pct"].apply(_clean_money)

    df["cep"] = df["endereco"].apply(_extract_cep)

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
        m = re.search(r"(\d+[.,]?\d*)\s*m[²2]", desc.lower())
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except ValueError:
                pass
        return None

    df["area_m2"] = df["descricao"].apply(_area)

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
    failed_ufs = []

    for i, uf in enumerate(UFS):
        df = fetch_uf(session, uf)
        if df is not None and not df.empty:
            frames.append(df)
        else:
            failed_ufs.append(uf)

        # Delay entre estados — aleatório, mais longo do que antes
        if i < len(UFS) - 1:
            wait = random.uniform(3, 7)
            log.debug("Aguardar %.1fs antes do próximo…", wait)
            time.sleep(wait)

    if not frames:
        log.error("Nenhum CSV foi descarregado. A abortar.")
        sys.exit(1)

    if failed_ufs:
        log.warning("Estados que falharam: %s", failed_ufs)

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
