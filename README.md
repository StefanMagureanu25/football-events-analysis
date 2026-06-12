# Football Events Analysis — Proiect Big Data

Proiect de master (Big Data, UB FMI, anul I) pe dataset-ul [Football Events](https://www.kaggle.com/datasets/secareanualin/football-events), aplicând **Apache Spark** (DataFrame API, Spark SQL, MLlib, Structured Streaming) și **TensorFlow** pe ~941K evenimente de fotbal.

## Structura proiectului

```
football-events-analysis/
├── notebooks/      # cele 7 notebook-uri (cod sursă, unul per cerință)
├── docs/           # documentație (teorie, cerințe, roadmap, resurse)
├── data/           # events.csv, ginf.csv, dictionary.txt (+ processed_shots/ generat)
├── models/         # generat la rulare: rf_pipeline_model/, tf_goal_predictor.keras, tf_scaler.joblib
├── plots/          # generat la rulare: figurile salvate
├── venv/           # virtual environment (nu se include în arhivă)
└── README.md
```

## Notebook-uri (`notebooks/`)

| Fișier | Cerință |
|--------|---------|
| `...Proiect_BigData.ipynb` | 1 — Introducere, dataset, obiective |
| `...Cod_sursa_cerinta_2.ipynb` | 2 — Spark SQL + DataFrame API (EDA, xG, corelație) |
| `...Cod_sursa_cerinta_3.ipynb` | 3 — MLlib: Random Forest + GBT (+ class imbalance, overfitting) |
| `...Cod_sursa_cerinta_4.ipynb` | 4 — Pipeline ETL → Parquet + Pipeline MLlib serializat |
| `...Cod_sursa_cerinta_5.ipynb` | 5 — UDF + Pandas UDF + benchmark + CrossValidator |
| `...Cod_sursa_cerinta_6.ipynb` | 6 — Rețea neuronală TensorFlow (+ PR curve) |
| `...Cod_sursa_cerinta_7.ipynb` | 7 — Structured Streaming + inferență ML |

## Documentație (`docs/`)

| Fișier | Conținut |
|--------|----------|
| `cerinte_proiect.md` | Ce face fiecare cerință + „ce să știi la prezentare" (Q&A) |
| `teorie_curs.md` | Teorie + papere fundamentale + întrebări de examen |
| `resurse_proiect.md` | Resurse de studiu legate de fiecare notebook |
| `LEARNING_ROADMAP.md` | Roadmap Data Engineering (backend → DE) |
| `PROMPT_OPUS.md` | Prompt-ul folosit pentru runda de îmbunătățiri |

## Setup și rulare

```bash
# din rădăcina proiectului
source venv/bin/activate
jupyter notebook        # sau: jupyter lab
```

Deschide notebook-urile din `notebooks/`. Fiecare notebook are o **celulă-bootstrap** la început care mută automat working directory-ul în rădăcina proiectului, astfel încât căile relative (`data/`, `models/`, `plots/`) funcționează indiferent de unde e pornit Jupyter.

### Ordinea de rulare
Există dependențe între notebook-uri (artefacte pe disc):

```
2 → 4 → 3 → 5 → 6 → 7
```

- **Cerința 4** generează `data/processed_shots/` (Parquet) și `models/rf_pipeline_model/`.
- **Cerința 5** și **Cerința 7** depind de aceste artefacte.
- **Cerința 6** generează `models/tf_goal_predictor.keras` și `models/tf_scaler.joblib`.

## Mediu
Python 3.10, PySpark 4.1.2, TensorFlow 2.16.2, scikit-learn, matplotlib, seaborn (Apple Silicon ARM64).

## Dataset
[Football Events — Kaggle](https://www.kaggle.com/datasets/secareanualin/football-events): `events.csv` (941.009 evenimente) + `ginf.csv` (10.112 meciuri). Cheia de join: `id_odsp`.
