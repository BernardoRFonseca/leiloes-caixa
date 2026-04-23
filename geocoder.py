"""
Geocoder por CEP usando BrasilAPI (gratuito, sem API key).

Versão robusta:
- Sai cedo se não houver CEPs no dataset
- Garante que as colunas lat/lon existem mesmo quando o cache está vazio
- Não rebenta o pipeline se a API estiver indisponível
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path(__file__).parent / "data"
CACHE_FILE = DATA_DIR / "cep_cache.parquet"
PARQUET_FILE = DATA_DIR / "imoveis.parquet"

BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"

MAX_WORKERS = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("geocoder")


def _normalize_cep(cep) -> str | None:
    if not cep or pd.isna(cep):
        return None
    s = "".join(c for c in str(cep) if c.isdigit())
    if len(s) != 8:
        return None
    return s


def lookup_cep(cep: str) -> dict:
    """Devolve sempre um dict, mesmo em caso de falha (lat/lon = None)."""
    cep_clean = _normalize_cep(cep)
    fallback = {"cep": cep, "lat": None, "lon": None, "fonte": "fail"}
    if not cep_clean:
        return fallback

    # 1. BrasilAPI v2 (com coordenadas)
    try:
        r = requests.get(BRASILAPI_URL.format(cep=cep_clean), timeout=10)
        if r.status_code == 200:
            d = r.json()
            coords = (d.get("location") or {}).get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")
            return {
                "cep": cep,
                "lat": float(lat) if lat else None,
                "lon": float(lon) if lon else None,
                "fonte": "brasilapi",
            }
    except Exception as e:
        log.debug("BrasilAPI falhou %s: %s", cep_clean, e)

    # 2. ViaCEP (sem coordenadas, mas confirma existência)
    try:
        r = requests.get(VIACEP_URL.format(cep=cep_clean), timeout=10)
        if r.status_code == 200 and not r.json().get("erro"):
            return {"cep": cep, "lat": None, "lon": None, "fonte": "viacep"}
    except Exception as e:
        log.debug("ViaCEP falhou %s: %s", cep_clean, e)

    return fallback


def _ensure_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que df tem colunas lat e lon (mesmo que vazias)."""
    if "lat" not in df.columns:
        df["lat"] = None
    if "lon" not in df.columns:
        df["lon"] = None
    return df


def main():
    if not PARQUET_FILE.exists():
        log.error("Não existe %s — corre primeiro o scraper.py", PARQUET_FILE)
        return

    df = pd.read_parquet(PARQUET_FILE)
    log.info("Carregados %d imóveis", len(df))

    if "cep" not in df.columns:
        log.warning("Coluna 'cep' não existe no parquet. A criar lat/lon vazios e sair.")
        df = _ensure_lat_lon(df)
        df.to_parquet(PARQUET_FILE, index=False)
        return

    ceps = df["cep"].dropna().unique().tolist()
    log.info("CEPs únicos no dataset: %d", len(ceps))

    if not ceps:
        log.warning("Nenhum CEP no dataset. A criar lat/lon vazios e sair.")
        df = _ensure_lat_lon(df)
        df.to_parquet(PARQUET_FILE, index=False)
        return

    # Carregar cache existente
    if CACHE_FILE.exists():
        cache = pd.read_parquet(CACHE_FILE)
        log.info("Cache existente: %d CEPs", len(cache))
        ja_em_cache = set(cache["cep"].tolist())
    else:
        cache = pd.DataFrame(columns=["cep", "lat", "lon", "fonte"])
        ja_em_cache = set()

    a_processar = [c for c in ceps if c not in ja_em_cache]
    log.info("CEPs novos a consultar: %d", len(a_processar))

    if a_processar:
        novos = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(lookup_cep, c): c for c in a_processar}
            for i, fut in enumerate(as_completed(futures), 1):
                novos.append(fut.result())
                if i % 200 == 0:
                    log.info("  Progresso: %d/%d", i, len(a_processar))

        novos_df = pd.DataFrame(novos)
        cache = pd.concat([cache, novos_df], ignore_index=True)
        cache.to_parquet(CACHE_FILE, index=False)
        log.info("Cache atualizado: %d CEPs", len(cache))

    # Merge defensivo
    if "lat" in df.columns:
        df = df.drop(columns=["lat"])
    if "lon" in df.columns:
        df = df.drop(columns=["lon"])

    df = df.merge(
        cache[["cep", "lat", "lon"]].drop_duplicates(subset=["cep"]),
        on="cep",
        how="left",
    )

    geocoded = df["lat"].notna().sum()
    log.info("Imóveis com coordenadas: %d / %d (%.1f%%)",
             geocoded, len(df), 100 * geocoded / len(df) if len(df) else 0)

    df.to_parquet(PARQUET_FILE, index=False)
    log.info("Parquet atualizado em %s", PARQUET_FILE)


if __name__ == "__main__":
    main()
