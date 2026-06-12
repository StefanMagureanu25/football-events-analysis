# Roadmap Data Engineering — pentru un Backend Engineer

> **Contextul tău:** Ești backend engineer, deci ai deja fundamente solide în sisteme distribuite, API-uri, baze de date și cod de producție. Asta e un avantaj **enorm** față de cineva care vine din analytics. Gândești deja în termeni de latență, throughput, fault tolerance și scalabilitate — exact ce contează în data engineering.

---

## Cum diferă Data Engineering de Backend Engineering

| Backend Engineer | Data Engineer |
|---|---|
| Servești date la request (OLTP) | Procesezi și transformi date în bulk (OLAP) |
| Latență: milisecunde | Latență: secunde → ore (batch) sau ms (streaming) |
| State în DB relațional | State în Data Lake / Data Warehouse |
| API → DB → response | Source → Pipeline → Sink |
| Verticile: un user, un request | Orizontale: milioane de rânduri simultan |
| Debug cu logs + traces | Debug cu DAG-uri + job metrics |

**Ce transferi direct din backend:** concurență, idempotență, retry logic, schema design, SQL, containerizare, CI/CD.

---

## Faza 1 — Fundamente (2-3 luni)
*Dacă știi deja SQL intermediar și Python, sari direct la Faza 2.*

### SQL avansat
Ești deja backend, deci SQL de bază îl știi. Ce îți lipsește probabil:
- **Window functions:** `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`, `NTILE()`
- **CTEs recursive** și query optimization (EXPLAIN ANALYZE)
- **Aggregate vs analytic functions** — diferența fundamentală
- **Slowly Changing Dimensions (SCD)** — cum versionezi datele istorice

```sql
-- Exemplu window function: ranking goluri per echipă per sezon
SELECT team, season, goals,
       RANK() OVER (PARTITION BY season ORDER BY goals DESC) AS rank_in_season
FROM team_stats;
```

📚 **Resurse:**
- [Mode SQL Tutorial](https://mode.com/sql-tutorial/) — gratuit, practic
- *SQL Antipatterns* — Bill Karwin (evită greșeli comune)
- [Use The Index, Luke](https://use-the-index-luke.com/) — optimizare query-uri

### Python pentru date
Ca backend engineer știi Python. Ce adaugi:
- **Pandas** — manipulare tabelară in-memory (știi deja din proiect)
- **NumPy** — operații vectoriale (evită loop-urile)
- **Generators și itertools** — pentru date mari care nu încap în RAM

⚠️ **Capcana backend-ului în data:** tentația de a scrie loops Python pe rânduri. Gândește vectorial, nu iterativ.

---

## Faza 2 — Core Data Engineering (3-6 luni)
*Aceasta e inima meseriei. Proiectul tău de Big Data acoperă ~60% din ce e aici.*

### Apache Spark (ai deja baza din proiect!)
Ce știi: DataFrame API, Spark SQL, MLlib, Streaming.  
Ce mai adaugi:

**Optimizare Spark (critical pentru producție):**
```python
# Partitionare corectă — cheia performanței
df.repartition(200)              # shuffle complet
df.coalesce(10)                  # reduce partitions fără shuffle
df.repartitionByRange(50, "date") # range-based, bun pentru time series

# Broadcast join — când o tabelă e mică
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "key")

# Caching strategic
df.cache()          # in-memory
df.persist(StorageLevel.DISK_ONLY)  # când RAM nu ajunge
```

**Catalyst Optimizer — ce se întâmplă când scrii un query:**
1. Unresolved Logical Plan → Resolved Logical Plan
2. Optimized Logical Plan (predicate pushdown, constant folding)
3. Physical Plans → Best Physical Plan (cost-based)
4. Code Generation (Tungsten)

Ca backend engineer, gândește-te la asta ca la query planner-ul din PostgreSQL, dar distribuit.

📚 **Resurse Spark:**
- *Learning Spark, 2nd Edition* (2020) — Damji et al., O'Reilly — **cea mai bună carte**
- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) — mai tehnic
- Databricks Academy — cursuri gratuite cu certificare
- [Spark Summit talks](https://www.youtube.com/c/Databricks) — YouTube, use cases reale

### Data Modeling
Ești obișnuit cu modele OLTP (3NF). Data engineering folosește altfel:

**Star Schema (Data Warehouse clasic):**
```
Fact Table: events (is_goal, time, location, bodypart)
    ↓
Dimension Tables: dim_team, dim_player, dim_match, dim_league
```

**Data Vault (pentru auditabilitate):**
- Hub (entități), Link (relații), Satellite (atribute în timp)
- Ideal când sursa de date se schimbă frecvent

**Medallion Architecture (Lakehouse — modern):**
```
Bronze → Silver → Gold
(raw)    (clean)  (aggregated/ready for ML)
```
Exact ce ai făcut tu în Cerința 4: CSV → Parquet = Bronze → Silver!

📚 **Resurse:**
- *The Data Warehouse Toolkit* — Kimball & Ross (biblia star schema)
- *Fundamentals of Data Engineering* — Reis & Housley, O'Reilly 2022 (**cea mai modernă**)

### ETL vs ELT
| ETL (clasic) | ELT (modern) |
|---|---|
| Transform înainte de load | Load raw, transform în warehouse |
| Potrivit când storage e scump | Potrivit când compute e scump (cloud) |
| Ex: Spark job → DW | Ex: S3 → Snowflake → dbt |

Ca backend engineer: ELT = `INSERT raw data, THEN run SQL transformations`. Mai simplu de debug.

---

## Faza 3 — Stack Modern de Data Engineering (3-6 luni)
*Asta te diferențiază pe piața muncii în 2025-2026.*

### Orchestrare — Apache Airflow
Gândește-te la Airflow ca la un **cron job pe steroizi**, cu dependențe între task-uri, retry, alerting și UI.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG('football_pipeline', schedule='@daily', start_date=datetime(2024,1,1)) as dag:
    
    extract = PythonOperator(task_id='extract_events', python_callable=extract_fn)
    transform = PythonOperator(task_id='transform_with_spark', python_callable=spark_fn)
    load = PythonOperator(task_id='load_to_warehouse', python_callable=load_fn)
    
    extract >> transform >> load  # dependențe
```

Ca backend engineer: un DAG = un workflow. Task-urile = microservicii care se apelează în ordine.

📚 *Data Pipelines with Apache Airflow* — Harenslak & de Ruiter, Manning 2021

### dbt (Data Build Tool)
**Cel mai hot tool în data engineering acum.** Transformi SQL în pipeline-uri versionabile cu teste automate.

```sql
-- models/shots_enriched.sql
WITH base AS (
    SELECT * FROM {{ ref('raw_events') }}
    WHERE event_type = 1
),
enriched AS (
    SELECT *,
           CASE WHEN location IN (3,5,10,12,14) THEN 'central'
                ELSE 'peripheral' END AS shot_zone
    FROM base
)
SELECT * FROM enriched
```

Ca backend engineer: dbt = migrations + ORM pentru analytics. Știi deja conceptul.

📚 [dbt Learn](https://courses.getdbt.com/) — gratuit, 4 ore

### Cloud Data Platforms
**Alege una și devino expert în ea:**

| Platformă | Strength | Unde e folosit |
|---|---|---|
| **Snowflake** | SQL pur, scalare automată | Finance, Enterprise |
| **Databricks** | Spark managed, ML integrat | Tech companies |
| **BigQuery** | Serverless, plătești per query | GCP shops |
| **Redshift** | AWS ecosystem | Amazon shops |

**Recomandare ca backend engineer:** Începe cu **BigQuery** (free tier generos, SQL familiar) sau **Databricks** (Spark pe care îl știi deja).

### Streaming — Kafka + Flink
Ai Spark Streaming din proiect. Pasul următor:

**Apache Kafka** — message broker distribuit:
```
Producer → Topic → Consumer Group
(app)       (queue) (Spark Streaming / Flink)
```
Ca backend engineer: Kafka = RabbitMQ / SQS, dar pentru volume masive cu replay.

**Apache Flink** — streaming mai avansat decât Spark Streaming:
- True streaming (nu micro-batch)
- Stateful operations cu checkpointing
- Folosit la: Uber, Alibaba, LinkedIn

📚 [Kafka: The Definitive Guide](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) — gratuit online

---

## Faza 4 — ML Engineering & MLOps (dacă vrei ML în producție)
*Relevanță directă cu ce ai făcut în proiect.*

### Diferența Data Engineer vs ML Engineer
- **Data Engineer:** construiește pipeline-urile care alimentează modelele
- **ML Engineer:** construiește, deployează și monitorizează modelele
- **MLOps Engineer:** automatizează întregul ciclu (training → deploy → monitor → retrain)

### Feature Stores
Componenta care lipsea din proiectul tău: un loc centralizat pentru features ML:
- **Feast** (open source) — features offline + online în aceeași interfață
- **Tecton** (managed) — folosit la Airbnb, Lyft

```python
# Conceptul: definești features o dată, le folosești peste tot
from feast import FeatureStore
store = FeatureStore(".")
features = store.get_online_features(
    features=["shot_stats:location", "shot_stats:bodypart"],
    entity_rows=[{"shot_id": "abc123"}]
)
```

### MLflow — tracking experimente
```python
import mlflow
with mlflow.start_run():
    mlflow.log_param("numTrees", 100)
    mlflow.log_param("maxDepth", 8)
    mlflow.log_metric("auc", 0.76)
    mlflow.sklearn.log_model(model, "random_forest")
```

Ca backend engineer: MLflow = structured logging pentru modele ML.

📚 *Designing Machine Learning Systems* — Chip Huyen, O'Reilly 2022 (**must read**)

---

## Faza 5 — Data Platform Architecture (senior level)
*Unde vrei să ajungi în 2-3 ani.*

### Lambda vs Kappa Architecture
**Lambda:** Batch layer + Speed layer + Serving layer  
**Kappa:** Doar streaming, tratezi batch ca streaming lent  
→ Modern: **Lakehouse** (Delta Lake, Apache Iceberg) — unifică batch + streaming

### Data Mesh
Paradigma nouă: tratezi datele ca produse, cu ownership distribuit pe domenii.
- Fiecare echipă e owner al datelor ei
- Data contracts între echipe
- Self-serve data platform

📚 *Data Mesh* — Zhamak Dehghani, O'Reilly 2022 (autoarea conceptului)

---

## Skilluri care te diferențiază pe piața muncii (2025-2026)

### Hard skills (ordonate după cerere)
1. **SQL avansat** (window functions, optimization) — cerut la orice job
2. **PySpark** — cerut la 70% din joburi DE
3. **Airflow sau Prefect** — orchestrare
4. **dbt** — transform layer
5. **Unul din:** Snowflake / Databricks / BigQuery
6. **Kafka** — streaming
7. **Delta Lake / Apache Iceberg** — table formats moderne

### Soft skills critice în data
- **Data modeling intuition** — nu doar "ce date colectăm" ci "ce întrebări trebuie să putem pune peste 2 ani"
- **Debugging distributed systems** — citit Spark UI, înțeles shuffle, detectat skew
- **Communication cu stakeholders** — data engineering fără utilizatori e infrastructure waste

### Ce faci tu deja (din proiect) față de un junior DE:
✅ Spark DataFrame + SQL  
✅ MLlib Pipeline cu serializare  
✅ Structured Streaming  
✅ Feature engineering custom (UDF)  
✅ TensorFlow pe date reale  
✅ ETL batch cu output Parquet  

**Ce îți mai lipsește pentru un junior DE role:**  
⬜ Airflow / Prefect (orchestrare)  
⬜ dbt (transform layer)  
⬜ Cloud platform (BigQuery / Databricks free tier)  
⬜ Kafka basics  
⬜ Data modeling (star schema practic)  

---

## Plan concret pentru tine (6-12 luni)

### Luna 1-2: Consolidare
- Rulează și înțelege în profunzime tot proiectul de Big Data
- Citește *Learning Spark, 2nd Edition* capitolele 1-8
- Fă cursul gratuit Databricks: [Apache Spark Programming with Databricks](https://www.databricks.com/learn/training/catalog)

### Luna 3-4: Orchestrare + dbt
- Instalează Airflow local (Docker Compose) și reproiectează pipeline-ul de fotbal ca DAG
- Fă cursul dbt Learn (4 ore, gratuit)
- Proiect: mută transformările din Cerința 2 în modele dbt

### Luna 5-6: Cloud
- Creează cont BigQuery (free tier: 10GB storage, 1TB queries/lună gratis)
- Încarcă football dataset în BigQuery și replică analizele din Cerința 2
- Sau: Databricks Community Edition (Spark managed, gratuit)

### Luna 7-9: Streaming real
- Instalează Kafka local (Docker)
- Conectează Kafka → Spark Structured Streaming (înlocuiește simularea din Cerința 7)
- Citește *Kafka: The Definitive Guide* (gratuit online)

### Luna 10-12: Portfolio
- Construiește un proiect end-to-end public pe GitHub:
  - Sursă: API real (ex: football-data.org — gratuit)
  - Ingestie: Kafka producer în Python
  - Procesare: Spark Structured Streaming
  - Storage: Delta Lake pe S3 sau BigQuery
  - Orchestrare: Airflow DAG
  - Transform: dbt models
  - Vizualizare: Metabase sau Grafana
- Scrie 2-3 articole pe Medium/dev.to despre ce ai construit

---

## Conexiuni: ce ai făcut deja în proiect vs conceptele din roadmap

Proiectul de Big Data nu e doar o temă de facultate — e **primul tău proiect de data engineering**. Iată maparea exactă concept ↔ notebook:

### ✅ Deja implementat în proiect

| Concept (roadmap) | Unde, concret | Notebook |
|---|---|---|
| DataFrame API + Spark SQL (Catalyst) | EDA, temp views, join events×ginf | C2 |
| Lazy evaluation, DAG, shuffle (wide/narrow) | groupBy/join pe ~1M rânduri | C2, C3 |
| Feature engineering | `is_central_shot`, `time_phase`, `is_set_piece`; xG empiric | C4, C2 |
| Parquet columnar + `partitionBy` | scriere `data/processed_shots` partiționat pe `situation` | C4 |
| Medallion Bronze→Silver | CSV brut → Parquet curățat | C4 |
| ETL pipeline | read → filter → clean → cast → feature → write | C4 |
| MLlib Pipeline (Estimator/Transformer) | Imputer → Assembler → Scaler → RF | C4 |
| Model serialization + reload | `PipelineModel.save/load`, `joblib` scaler | C4, C6 |
| Hyperparameter tuning (k-fold CV) | `CrossValidator`, grid de 24 antrenări | C5 |
| UDF + vectorizare (Arrow) | `shot_quality_score` + `pandas_udf` + benchmark | C5 |
| Class imbalance | `weightCol` (MLlib), `class_weight` (Keras) | C3, C6 |
| Model evaluation | AUC, PR curve, confusion matrix, overfitting check | C3, C6 |
| Deep Learning | Keras Dense + BatchNorm + Dropout | C6 |
| Structured Streaming | micro-batch, output modes, checkpoint | C7 |
| Inferență offline→online (Lambda) | model antrenat în C4, aplicat pe stream în C7 | C7 |

### ⬜ Următorul pas natural (pornind de la EXACT acest proiect)

| Concept lipsă | Cum îl adaugi pe proiectul existent |
|---|---|
| Orchestrare (Airflow) | transformă ordinea de rulare `2→4→3→5→6→7` într-un DAG cu dependențe |
| dbt | mută query-urile SQL din C2 în modele dbt versionabile, cu teste |
| Kafka | înlocuiește simularea cu fișiere din C7 cu un producer Kafka real |
| Cloud (BigQuery/Databricks) | încarcă dataset-ul și rulează analizele din C2 în cloud |
| MLflow | loghează rulările de tuning din C5 ca experimente urmăribile |
| Feature Store (Feast) | centralizează features-urile din C4 pentru reutilizare offline+online |

---

## 3 proiecte de portofoliu (beginner → advanced)

Fiecare pornește de la ce știi deja și adaugă **exact piesele care lipsesc** pentru un rol de Data Engineer.

### 🟢 Beginner — „Orchestrated Batch ELT" (~3-4 săptămâni)
**Scop:** pipeline batch zilnic dintr-un API public spre un warehouse, cu transformări versionate și un dashboard.
- **Sursă:** API public (football-data.org gratuit, OpenWeather, sau un API crypto)
- **Ingestie:** script Python idempotent (zona ta de confort din backend)
- **Orchestrare:** Airflow local (Docker Compose), DAG `@daily`
- **Storage:** DuckDB (local) sau BigQuery (free tier)
- **Transform:** dbt (staging → marts, cu teste `not_null`/`unique`)
- **Dashboard:** Streamlit sau Metabase

**Demonstrează:** orchestrare, ELT, data modeling de bază, scheduling, idempotență.
**Stack pe CV:** Python, Airflow, dbt, BigQuery/DuckDB, SQL.

### 🟡 Intermediate — „Real-Time Lakehouse" (~5-6 săptămâni)
**Scop:** upgradezi Cerința 7 de la simulare cu fișiere la **streaming real** cu arhitectură medallion.
- **Sursă:** Kafka producer Python (simulează evenimente sau pollează un API real)
- **Procesare:** PySpark Structured Streaming (consumer)
- **Storage:** Delta Lake pe MinIO/S3 — straturile Bronze/Silver/Gold
- **Batch gold layer:** Airflow DAG care agregă Silver → Gold
- **Dashboard:** Grafana pe stratul Gold

**Demonstrează:** streaming real (nu micro-batch din fișiere), exactly-once, medallion, table formats moderne.
**Stack pe CV:** Kafka, Spark Structured Streaming, Delta Lake, S3/MinIO, Airflow.

### 🔴 Advanced — „End-to-End ML Platform cu MLOps" (~2-3 luni)
**Scop:** platformă completă, de la ingestie la model în producție, cu monitorizare.
- **Ingestie:** Kafka + Airbyte (CDC dintr-un Postgres)
- **Transform:** dbt + Spark (medallion)
- **Feature Store:** Feast (features offline + online)
- **Training + tracking:** MLflow (reia modelele din C3/C6)
- **Serving:** FastAPI care servește predicții (din nou, zona ta de confort backend!)
- **Monitoring:** Evidently AI (data drift, model drift)
- **CI/CD:** GitHub Actions; **IaC:** Terraform; **Cloud:** Databricks sau AWS

**Demonstrează:** ciclu MLOps complet — exact ce cer rolurile mid/senior ML/Data Engineer.
**Stack pe CV:** Kafka, dbt, Feast, MLflow, FastAPI, Evidently, Terraform, Databricks/AWS, GitHub Actions.

> **Sfat:** publică fiecare proiect pe GitHub cu README bun (diagramă de arhitectură + cum se rulează) și scrie un articol pe dev.to/Medium. Un portofoliu cu 2-3 proiecte end-to-end bate orice listă de certificări.

---

## Ce cer interviurile de Data Engineer (România + remote)

> Ca backend engineer cu acest proiect + 1-2 proiecte de portofoliu, poți targeta direct **mid-level**, nu junior.

### Structura tipică a unui loop de interviu DE
1. **Screening SQL** (live coding) — window functions, agregări, optimizare
2. **Take-home / live** — construiește un mic pipeline sau debug unul existent
3. **System design** — „proiectează un pipeline pentru X" (mai greu la senior)
4. **Behavioral** — colaborare, ownership, comunicare cu stakeholderi

### Junior DE
**Ce se testează:**
- **SQL** intens (window functions, CTEs, optimizare) — aproape sigur live coding
- **Python** (pandas, structuri de date, scripting curat)
- Un tool distribuit la nivel de bază (**Spark** — exact ce ai din proiect)
- Data modeling de bază (normalizare, star schema)
- Un warehouse (BigQuery/Snowflake/Redshift — măcar unul)
- Git, noțiuni CI/CD

**Întrebări tipice:** „Scrie un query care găsește top 3 X per grup", „Diferența între `WHERE` și `HAVING`", „Ce e un shuffle în Spark", „Cum tratezi duplicatele într-un pipeline".

### Senior DE
**Ce se testează în plus:**
- **Optimizare Spark profundă** — shuffle, data skew, broadcast join, citit Spark UI
- **Streaming** real (Kafka + Spark/Flink), semantici exactly-once
- **Data modeling la scară** — SCD, data vault, dimensional modeling
- **Cloud** expert pe o platformă + **IaC** (Terraform)
- **System design** — „pipeline pentru 5 TB/zi, optimizat pe cost, cu SLA"
- **Leadership** — mentoring, decizii de arhitectură, comunicare cu business

**Întrebări tipice:** „Pipeline-ul e lent, cum diagnostichezi?", „Cum garantezi exactly-once end-to-end?", „Batch vs streaming pentru use-case-ul X și de ce?", „Cum versionezi datele istorice (SCD)?".

### Specific România
- **Companii care angajează DE:** UiPath, Bitdefender, eMAG, bănci (ING, BCR, Raiffeisen), Endava, Luxoft, Accenture, Adobe RO, Microsoft RO, Continental, consultanțe.
- **Salarii orientative (net/lună, ~2025-2026, variază mult):** Junior ~5.000–9.000 RON, Mid ~10.000–16.000 RON, Senior ~17.000–28.000+ RON.
- Multe roluri apar ca „Data Engineer" în echipe de analytics/BI sau direct în product.

### Specific Remote (companii EU/US care angajează din RO)
- **Bar mai înalt pe system design + comunicare** (async, engleză scrisă bună).
- Adesea ca **contractor** (prin Deel/Remote.com) sau angajat printr-o entitate locală.
- **Salarii orientative (EUR/lună, contractor, variază mult):** Junior ~€2.000–3.500, Mid ~€3.500–6.000, Senior ~€6.000–9.000+.
- Procese mai lungi (3-5 etape), take-home mai serios, accent pe ownership și autonomie.

> **Caveat:** cifrele de salariu sunt aproximative și se schimbă rapid — folosește-le doar ca ordin de mărime. Verifică surse curente (levels.fyi, Glassdoor, grupuri locale de IT).

---

## Comunități și resurse de urmărit

### Newsletter-uri (săptămânale, ~10 min citit)
- [Data Engineering Weekly](https://www.dataengineeringweekly.com/)
- [Bytes & Bytes](https://bytesandbytes.substack.com/)
- [The Sequence](https://thesequence.substack.com/) — ML + DE

### YouTube
- [Seattle Data Guy](https://www.youtube.com/c/SeattleDataGuy) — practic, real-world
- [Andreas Kretz](https://www.youtube.com/c/andreaskayy) — Data Engineering Podcast
- [Databricks](https://www.youtube.com/c/Databricks) — Spark Summit talks

### Cărți în ordinea priorității
1. *Fundamentals of Data Engineering* — Reis & Housley (2022) — **START AICI**
2. *Learning Spark, 2nd Ed.* — Damji et al. (2020)
3. *Designing Machine Learning Systems* — Chip Huyen (2022)
4. *The Data Warehouse Toolkit* — Kimball (modeling clasic)
5. *Streaming Systems* — Akidau et al. (streaming avansat)

### Certificări care contează pe CV
- **Databricks Certified Associate Developer for Apache Spark** — cea mai recunoscută pentru Spark
- **Google Professional Data Engineer** — cloud + pipeline design
- **dbt Analytics Engineer Certification** — în creștere ca relevantă

---

## Concluzie

Ca backend engineer care intră în data engineering, ai deja ~40% din skilluri. Ce trebuie să schimbi fundamental:

> **Gândești la un request → un response.  
> Trebuie să înveți să gândești la milioane de rânduri → un insight.**

Proiectul tău de Big Data e un fundament solid. Continuă pe el, adaugă orchestrare și un cloud platform, și în 6-9 luni poți aplica cu încredere pentru roluri de Data/ML Engineer.
