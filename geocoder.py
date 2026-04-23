"""
Geocoder por CEP usando BrasilAPI (gratuito, sem necessidade de API key).

Estratégia:
  1. Carrega o parquet existente
  2. Para cada CEP único ainda sem coordenadas, consulta BrasilAPI v2
     (que devolve latitude/longitude via integração com o WikiData/Google)
  3. Cache local em data/cep_cache.parquet para nunca repetir consultas
  4. Faz merge das coordenadas de volta para o parquet principal

Para 30k imóveis costuma haver ~5-8k CEPs distintos.
Com rate-limit conservador (10/s) demora ~15 minutos na primeira corrida.
Em corridas seguintes só processa CEPs novos.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path(__file__).parent / "data"
CACHE_FILE = DATA_DIR / "cep_cache.parquet"
PARQUET_FILE = DATA_DIR / "imoveis.parquet"

BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"  # fallback

MAX_WORKERS = 8  # paralelismo modesto para não sermos bloqueados

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("geocoder")


def _normalize_cep(cep: str) -> str | None:
    """Limpa CEP para formato 8 dígitos."""
    if not cep:
        return None
    s = "".join(c for c in str(cep) if c.isdigit())
    if len(s) != 8:
        return None
    return s


def lookup_cep(cep: str) -> dict | None:
    """
    Faz lookup de um CEP. Tenta BrasilAPI v2 (que tem coordenadas),
    cai para ViaCEP se falhar (sem coordenadas, mas com bairro/cidade).
    """
    cep_clean = _normalize_cep(cep)
    if not cep_clean:
        return None

    # 1. BrasilAPI v2 (tem location.coordinates)
    try:
        r = requests.get(BRASILAPI_URL.format(cep=cep_clean), timeout=10)
        if r.status_code == 200:
            d = r.json()
            coords = (d.get("location") or {}).get("coordinates") or {}
            return {
                "cep": cep,
                "lat": float(coords["latitude"]) if coords.get("latitude") else None,
                "lon": float(coords["longitude"]) if coords.get("longitude") else None,
                "cep_cidade": d.get("city"),
                "cep_uf": d.get("state"),
                "cep_bairro": d.get("neighborhood"),
                "fonte": "brasilapi",
            }
    except Exception as e:
        log.debug("BrasilAPI falhou para %s: %s", cep_clean, e)

    # 2. Fallback ViaCEP (sem coordenadas)
    try:
        r = requests.get(VIACEP_URL.format(cep=cep_clean), timeout=10)
        if r.status_code == 200:
            d = r.json()
            if not d.get("erro"):
                return {
                    "cep": cep,
                    "lat": None,
                    "lon": None,
                    "cep_cidade": d.get("localidade"),
                    "cep_uf": d.get("uf"),
                    "cep_bairro": d.get("bairro"),
                    "fonte": "viacep",
                }
    except Exception as e:
        log.debug("ViaCEP falhou para %s: %s", cep_clean, e)

    return {"cep": cep, "lat": None, "lon": None, "fonte": "fail"}


def main():
    if not PARQUET_FILE.exists():
        log.error("Não existe %s — corre primeiro o scraper.py", PARQUET_FILE)
        return

    df = pd.read_parquet(PARQUET_FILE)
    log.info("Carregados %d imóveis", len(df))

    # CEPs únicos a processar
    ceps = df["cep"].dropna().unique().tolist()
    log.info("CEPs únicos no dataset: %d", len(ceps))

    # Carrega cache se existir
    if CACHE_FILE.exists():
        cache = pd.read_parquet(CACHE_FILE)
        log.info("Cache existente: %d CEPs", len(cache))
        ja_em_cache = set(cache["cep"].tolist())
    else:
        cache = pd.DataFrame()
        ja_em_cache = set()

    a_processar = [c for c in ceps if c not in ja_em_cache]
    log.info("CEPs novos a consultar: %d", len(a_processar))

    if a_processar:
        novos = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(lookup_cep, c): c for c in a_processar}
            for i, fut in enumerate(as_completed(futures), 1):
                result = fut.result()
                if result:
                    novos.append(result)
                if i % 100 == 0:
                    log.info("  Progresso: %d/%d", i, len(a_processar))

        novos_df = pd.DataFrame(novos)
        cache = pd.concat([cache, novos_df], ignore_index=True) if not cache.empty else novos_df
        cache.to_parquet(CACHE_FILE, index=False)
        log.info("Cache atualizado: %d CEPs", len(cache))

    # Merge coordenadas para o parquet principal
    df = df.merge(
        cache[["cep", "lat", "lon"]],
        on="cep",
        how="left",
    )

    geocoded = df["lat"].notna().sum()
    log.info("Imóveis com coordenadas: %d / %d (%.1f%%)",
             geocoded, len(df), 100 * geocoded / len(df))

    df.to_parquet(PARQUET_FILE, index=False)
    log.info("Parquet atualizado em %s", PARQUET_FILE)


if __name__ == "__main__":
    main()
