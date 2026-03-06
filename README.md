# Data Pipeline (Bronze → Silver → Gold) — Clima + “Energia”

Um mini-projeto de **Engenharia de Dados** em Python, com cara de “data platform”:
- **Ingestão** de dados de clima via **Open-Meteo API** (grátis, sem key)
- Persistência em **camadas**:
  - **Bronze**: dados raw (JSON + Parquet)
  - **Silver**: dados limpos/normalizados
  - **Gold**: agregações prontas pra BI (ex.: médias e totais por mês/cidade)
- **Data Quality** com **Pandera** (schema + regras)
- **Orquestração** com **Prefect**
- **Carga em Postgres** via Docker
- **Testes + CI** (pytest + GitHub Actions)

> Objetivo: servir como portfólio para vaga de **Engenheiro de Dados**.

---

## Arquitetura (resumo)

**Open-Meteo API** → (extract) → **Bronze** → (transform + quality) → **Silver** → (aggregate) → **Gold**  
Além disso: Silver + Gold são carregados no **Postgres** para consultas SQL.

---

## Como rodar (Windows / Linux / Mac)

### 1) Subir o Postgres
```bash
docker compose up -d
```

### 2) Instalar dependências
Crie um venv (recomendado) e instale:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
# source .venv/bin/activate

pip install -r requirements.txt -r requirements-dev.txt
```

### 3) Configurar variáveis (opcional)
Copie o arquivo de exemplo:
```bash
cp .env.example .env
```
No Windows:
```powershell
copy .env.example .env
```

### 4) Rodar o pipeline (Bronze → Silver → Gold)
```bash
python -m src.pipeline.flows
```

Ao final, você vai ter:
- `data/bronze/` (json + parquet)
- `data/silver/` (parquet limpo)
- `data/gold/` (parquets agregados)
- tabelas no Postgres: `bronze_weather_daily`, `silver_weather_daily`, `gold_city_monthly`

---

## Consultas SQL (exemplos)

Conecte no Postgres (host `localhost`, porta `5432`, user `postgres`, senha `postgres`, db `weather_energy`).

Exemplo:
```sql
SELECT city, month, avg_temperature_2m_mean_c, total_precip_mm
FROM gold_city_monthly
ORDER BY city, month;
```

---

## O que esse projeto demonstra (pra recrutador)

- Camadas **Bronze/Silver/Gold**
- Padronização e validação de dados (Data Quality)
- Escrita em **Parquet**
- Carga e modelagem em **Postgres**
- Orquestração com **Prefect**
- Testes + CI

---

## Ideias de upgrade (se quiser evoluir)
- Incremental load (somente dias novos)
- Particionamento de Parquet (por `city` e `date`)
- dbt para modelagem (silver/gold)
- Métricas/observabilidade (Prometheus/Grafana)
- Deploy (Dockerfile + Prefect server)

---

## Licença
MIT
