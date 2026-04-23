"""
Debug: descarrega 1 CSV de exemplo e mostra os primeiros 2000 caracteres.
Para perceber o que a Caixa está realmente a devolver.
"""
import requests

url = "https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_AC.csv"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Referer": "https://venda-imoveis.caixa.gov.br/sistema/download-lista.asp",
}

r = requests.get(url, headers=headers, timeout=30)

print(f"=== HTTP Status: {r.status_code} ===")
print(f"=== Content-Type: {r.headers.get('Content-Type')} ===")
print(f"=== Content-Length: {len(r.content)} bytes ===")
print(f"=== Encoding detetado: {r.encoding} ===")
print()
print("=== PRIMEIROS 2000 CARACTERES (latin-1) ===")
print(r.content[:2000].decode("latin-1", errors="replace"))
print()
print("=== ÚLTIMOS 500 CARACTERES ===")
print(r.content[-500:].decode("latin-1", errors="replace"))
