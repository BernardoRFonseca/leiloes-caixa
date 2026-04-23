# 🏠 Leilões Caixa — Dashboard

Dashboard que agrega, mapeia e permite explorar todos os imóveis disponíveis para venda/leilão na Caixa Econômica Federal do Brasil.

**Fonte:** CSVs oficiais publicados pela Caixa em `venda-imoveis.caixa.gov.br/listaweb/`, atualizados diariamente.

## 🏗️ Arquitetura

```
┌───────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│  Caixa CSVs   │ →  │  scraper.py  │ →  │ geocoder.py │ →  │   app.py     │
│  (27 estados) │    │  (download   │    │  (CEP →     │    │  (Streamlit  │
│               │    │   + clean)   │    │   lat/lon)  │    │   dashboard) │
└───────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
                              ↓                  ↓
                       data/imoveis.parquet  data/cep_cache.parquet
```

**Automação:** GitHub Actions corre o pipeline todos os dias às 06:00 UTC e faz commit dos dados atualizados.

## 🚀 Como correr localmente

```bash
# 1. Clone & setup
git clone <este-repo>
cd leiloes-caixa
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Descarregar e processar dados (primeira vez ~15-20 min com geocoder)
python scraper.py
python geocoder.py

# 3. Lançar dashboard
streamlit run app.py
```

Abre em `http://localhost:8501`.

## ☁️ Deploy gratuito (Streamlit Community Cloud)

1. Cria um repo no GitHub e faz push deste código
2. Vai a [share.streamlit.io](https://share.streamlit.io) → "New app"
3. Aponta para o teu repo, branch `main`, ficheiro `app.py`
4. Deploy. Em 2 minutos tens o dashboard online.

A automação dos dados (cron diário) já está incluída em `.github/workflows/update.yml` — só precisas de garantir que a Action tem permissão de write no repo (Settings → Actions → General → Workflow permissions → "Read and write").

## 📊 Features do MVP

- ✅ Lista filtrada (UF, cidade, tipo, preço, desconto, modalidade, quartos)
- ✅ Mapa interativo com pins por imóvel
- ✅ Drill-down por imóvel (foto, descrição, link para a página da Caixa)
- ✅ KPIs (mediana de preço/desconto, contagens)
- ✅ Estatísticas (distribuição por UF, tipo, preço, desconto)
- ✅ Atualização automática diária via GitHub Actions
- ⏳ Próximos passos sugeridos:
  - Histórico de preços (mesma casa em vários leilões) — exige snapshot diário em vez de overwrite
  - Alertas Telegram (novo imóvel que bata critérios guardados)
  - ROI estimado (cruzar com mediana de aluguel da região via FipeZAP)
  - Geocoding preciso por endereço (Google Maps API, $5/1000)

## 📁 Estrutura do projeto

```
.
├── scraper.py              # Download e normalização dos CSVs
├── geocoder.py             # Resolução de CEP → lat/lon
├── app.py                  # Dashboard Streamlit
├── requirements.txt
├── data/                   # (gerado) imoveis.parquet, cep_cache.parquet
├── .github/workflows/
│   └── update.yml          # Cron diário GitHub Actions
└── README.md
```

## 🔧 Notas técnicas

- **Encoding dos CSVs**: latin-1, separador `;`, primeiras 5 linhas são header descritivo
- **Geocoding**: BrasilAPI v2 (gratuito) com fallback para ViaCEP. ~70-90% dos CEPs têm coordenadas exatas; o resto fica sem pin no mapa mas continua na lista
- **Cache**: o `cep_cache.parquet` evita re-consultar CEPs já vistos. Em corridas seguintes só processa CEPs novos
- **Performance**: o parquet final ronda os 5-10 MB para 30k imóveis. Streamlit carrega-o em <1s

## ⚠️ Disclaimer

Este projeto consome dados públicos da Caixa Econômica Federal. Não é afiliado nem endossado pela Caixa. Sempre verifica a informação original no site oficial antes de fazer qualquer proposta.
