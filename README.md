# BlockStream

Welcome to **BlockStream** — a daily crypto pipeline that extracts, transforms, and visualizes crypto market data.  
This project automates data collection, runs daily, generates AI-powered weekly insights, and delivers them through a Streamlit dashboard and automated email bulletins.  
Stay informed on top movers, droppers, and key market trends — all fully automated and documented.

---

## 📆 Project Timeline

- ✅ **Week 1:** Data source identified and extract script working.
- ✅ **Week 2:** Load to database and transformation logic stable.
- 🔜 **Week 3:** Orchestration and monitoring live with automated alerts.
- 🔜 **Week 4:** Streamlit dashboard & subscription email bulletin launched.

---

## ⚙️ Pipeline Overview

| Stage | Description | Tech Options | Pros / Cons | Selected Tech | Status | Notes |
|-------|--------------|---------------|--------------|----------------|--------|-------|
| **1. Source** | Identify crypto data source (prices, volumes, news) | - [ ] CoinGecko API  <br> - [ ] Binance API  <br> - [ ] Alpha Vantage | ✅ CoinGecko: free, broad data <br> ❌ Rate limits  <br> ✅ Binance: real-time trades <br> ❌ Needs API keys | Binance (US) API | ✅ Selected | |
| **2. Extract** | Script to pull daily snapshot | - [ ] Python requests  <br> - [ ] Airbyte | ✅ Python: simple, flexible <br> ❌ Manual retries <br> ✅ Airbyte: managed connectors <br> ❌ Heavier infra | Python requests | ✅ Completed | |
| **3. Load** | Store raw data | - [ ] Postgres  <br> - [ ] DuckDB  <br> - [ ] S3/Parquet | ✅ Postgres: familiar SQL <br> ❌ Needs server <br> ✅ DuckDB: zero config, local files <br> ❌ Not great for large multi-user <br> ✅ S3: cheap, scalable <br> ❌ Needs extra query layer | DuckDB | ✅ Completed | Local file storage for daily dev |
| **4. Transform** | Clean & model data | - [ ] dbt  <br> - [ ] Pure SQL scripts | ✅ dbt: versioned, tested models <br> ❌ Learning curve <br> ✅ SQL: simple for small jobs <br> ❌ Harder to test/deploy | dbt | In Progress | dbt Core locally |
| **5. Orchestration** | Automate daily run | - [ ] cron job  <br> - [ ] GitHub Actions  <br> - [ ] Airflow  <br> - [ ] Prefect | ✅ cron: simple <br> ❌ No UI, logs <br> ✅ GitHub Actions: Off-the-shelf <br> ✅ Airflow: powerful, robust <br> ❌ Heavy <br> ✅ Prefect: modern, easy cloud UI <br> ❌ Freemium | GitHub Actions | In Progress | Start simple, upgrade later |
| **6. Visualization** | Build dashboard | - [ ] Streamlit  <br> - [ ] Dash  <br> - [ ] Superset | ✅ Streamlit: fast, easy deploy <br> ❌ Limited design <br> ✅ Dash: flexible callbacks <br> ❌ More code <br> ✅ Superset: no-code dashboards <br> ❌ Less custom | Streamlit | Planned | Weekly insights builder |
| **7. Monitoring & Alerts** | Track runs & failures | - [ ] Airflow alerts  <br> - [ ] Custom Python email | ✅ Airflow: built-in <br> ❌ Needs infra <br> ✅ Python: simple SMTP <br> ❌ Needs handling | Python SMTP | Planned | Basic alert on failure |
| **8. Weekly Email Bulletin** | Auto-insights with AI | - [ ] OpenAI GPT  <br> - [ ] Simple template | ✅ GPT: dynamic text <br> ❌ Cost <br> ✅ Template: cheap, fixed copy <br> ❌ Less engaging | OpenAI GPT | Planned | Weekly top movers/droppers |
| **9. Subscription System** | User signup | - [ ] Mailchimp  <br> - [ ] Custom DB + SMTP | ✅ Mailchimp: easy compliance <br> ❌ Freemium caps <br> ✅ Custom: full control <br> ❌ More work | Mailchimp (v1) | Planned | Add sign-up form in Streamlit |

---

## 📣 Stay Connected

[![Read on Medium](https://img.shields.io/badge/Read%20on-Medium-black?logo=medium)](https://upandtothewrite.medium.com/)
[![Find Me on LinkedIn](https://img.shields.io/badge/Connect-LinkedIn-blue?logo=linkedin)](https://www.linkedin.com/in/chrisadan/)

---

## 📚 Glossary

**API** — Application Programming Interface, how the pipeline fetches crypto data.  
**dbt** — Data Build Tool, for transforming and version-controlling SQL models.  
**DuckDB** — Lightweight local SQL OLAP database, great for single-node analytics.  
**cron** — Unix scheduler for running scripts automatically.  
**Airflow** — Workflow orchestration platform with a UI and task tracking.  
**Prefect** — Modern orchestration tool for data pipelines, with cloud scheduling.  
**Streamlit** — Python framework for building interactive data apps.  
**SMTP** — Simple Mail Transfer Protocol, used to send emails from scripts.  
**GPT** — Generative Pre-trained Transformer (e.g., OpenAI), used for AI-written insights.  
**Mailchimp** — Email marketing tool for managing subscribers and newsletters.

---
