# Resurse pentru a înțelege în profunzime acest proiect

> Spre deosebire de `teorie_curs.md` (teorie + papere) și `LEARNING_ROADMAP.md` (carieră DE), aici resursele sunt legate **direct de ce face fiecare notebook**. Pentru fiecare concept folosit în proiect ai cea mai bună 1-3 resurse, marcate `[FREE]` unde sunt gratuite, cu capitolul/episodul exact.

## Cum să folosești lista
1. Pornește de la notebook-ul pe care vrei să-l înțelegi.
2. Citește/vizionează resursa marcată ⭐ (cea mai bună pentru începutul tău).
3. Lasă papere-le din `teorie_curs.md` la final — sunt pentru „de ce", nu pentru „cum".

---

## Fundamente Spark (C2, C3, C4) — citește astea ÎNTÂI

| Resursă | Ce acoperă | Unde |
|---|---|---|
| ⭐ **Learning Spark, 2nd Edition** (Damji et al., O'Reilly 2020) `[FREE]` | Cap. 1-3 (arhitectură, DataFrame, SparkSession), cap. 4-5 (Spark SQL), cap. 7 (optimizare) | ebook gratuit oferit de Databricks (databricks.com → resources → ebook) |
| **Spark — Quick Start & SQL Programming Guide** `[FREE]` | Referință oficială pentru API-urile din C2 | https://spark.apache.org/docs/latest/ |
| **The Internals of Apache Spark** — Jacek Laskowski `[FREE]` | Catalyst, DAG, shuffle „sub capotă" (avansat) | https://books.japila.pl/apache-spark-internals/ |

**Mapare pe proiect:** tot ce e `spark.read`, `groupBy`, `join`, `createOrReplaceTempView` în C2 e explicat în Learning Spark cap. 3-5.

---

## Catalyst, lazy evaluation, shuffle (C2, C3)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **„Deep Dive into Spark SQL's Catalyst Optimizer"** — blog Databricks `[FREE]` | Cum devine query-ul tău un plan fizic optimizat | caută titlul pe databricks.com/blog |
| **Talk: „A Deep Dive into Catalyst"** — Yin Huai (Spark Summit) `[FREE]` | Aceeași temă, vizual | YouTube, canalul Databricks |

**De ce contează pentru tine:** explică DE CE Spark SQL și DataFrame API din C2 au aceeași performanță (analogie ta: query planner PostgreSQL distribuit).

---

## Parquet & stocare columnară (C4)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **„The Columnar Roadmap: Apache Parquet and Apache Arrow"** — blog/talk `[FREE]` | De ce columnar = compresie + column pruning + predicate pushdown | YouTube / blog (caută titlul) |
| **Documentația Apache Parquet** `[FREE]` | Formatul fizic, row groups, encoding | https://parquet.apache.org/docs/ |
| **Paper „Dremel"** (Google, 2010) | Originea formatului columnar de care depinde Parquet | Google Research (PDF) |

**Mapare pe proiect:** `partitionBy('situation')` + scrierea Parquet din C4 — vezi cum partition pruning sare peste foldere irelevante.

---

## Arbori de decizie, Random Forest, GBT (C3, C4, C5)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **StatQuest (Josh Starmer)** `[FREE]` | Playlist: Decision Trees, Random Forests, Gradient Boost (Part 1-4), AdaBoost, ROC & AUC | https://www.youtube.com/@statquest |
| ⭐ **An Introduction to Statistical Learning (ISLR)** — James et al. `[FREE]` | Cap. 8 (arbori, bagging, RF, boosting). Standardul de aur, cu intuiție + matematică ușoară | https://www.statlearning.com |
| **MLlib — Ensembles guide** `[FREE]` | Cum sunt implementate RF/GBT în Spark exact | https://spark.apache.org/docs/latest/ml-classification-regression.html |

**Mapare pe proiect:** StatQuest „Random Forests" + „Gradient Boost" îți explică EXACT diferența bagging vs boosting din C3 (RF vs GBT). „ROC & AUC" îți explică metrica din C3/C6.

---

## UDF, Pandas UDF, Apache Arrow (C5)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **„Introducing Pandas UDFs for PySpark"** — blog Databricks `[FREE]` | De ce pandas_udf > udf (Arrow, vectorizare) — exact benchmark-ul tău | databricks.com/blog (caută titlul) |
| **PySpark docs — pandas_udf** `[FREE]` | API-ul, tipurile de pandas UDF (Series→Series etc.) | https://spark.apache.org/docs/latest/api/python/ → user guide „pandas API on Spark" / „Pandas UDFs" |
| **Apache Arrow — overview** `[FREE]` | De ce formatul columnar permite transfer zero-copy JVM↔Python | https://arrow.apache.org/overview/ |

**Mapare pe proiect:** explică de ce, în C5, varianta `@pandas_udf` e mai rapidă decât `@udf` (problema N+1 vs batch).

---

## Cross-Validation & hyperparameter tuning (C5)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **StatQuest — „Machine Learning Fundamentals: Cross Validation"** `[FREE]` | k-fold în 6 minute | YouTube @statquest |
| **ISLR cap. 5** (Resampling Methods) `[FREE]` | De ce CV e mai robust decât un singur split | statlearning.com |

**Mapare pe proiect:** `CrossValidator` + `ParamGridBuilder` din C5 (8 combinații × 3 folds = 24 antrenări).

---

## Rețele neuronale & TensorFlow/Keras (C6)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **3Blue1Brown — „Neural Networks"** (4 episoade) `[FREE]` | Cea mai bună intuiție vizuală pentru forward pass, backpropagation, gradient descent | https://www.3blue1brown.com/topics/neural-networks |
| ⭐ **Hands-On Machine Learning** (Géron, 3rd ed.) | Cap. 10-11: Keras Sequential, antrenarea DNN, BatchNorm, Dropout, callbacks — exact arhitectura ta | carte O'Reilly |
| **Keras Developer Guides** `[FREE]` | Sequential model, training & evaluation | https://keras.io/guides/ |
| **Google ML Crash Course** `[FREE]` | Secțiunile „Neural Networks", „Classification" (prag, ROC, PR) | https://developers.google.com/machine-learning/crash-course |

**Mapare pe proiect:** Géron cap. 10-11 acoperă fiecare strat din C6 (Dense/BatchNorm/Dropout/sigmoid, Adam, EarlyStopping, class_weight).

---

## Dezechilibru de clase & Precision-Recall (C3, C6)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **Machine Learning Mastery — imbalanced classification** (Jason Brownlee) `[FREE]` | De ce accuracy minte pe date dezechilibrate, class weights, PR vs ROC | machinelearningmastery.com (caută „imbalanced classification") |
| **scikit-learn — „Precision-Recall"** `[FREE]` | Definiția curbei PR + average precision (exact ce ai în C6) | https://scikit-learn.org/stable/auto_examples/model_selection/plot_precision_recall.html |

**Mapare pe proiect:** justifică `weightCol` din C3 și curba PR + `class_weight` din C6.

---

## Structured Streaming (C7)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **Structured Streaming Programming Guide** `[FREE]` | Tabela infinită, output modes (complete/append/update), trigger, checkpoint, watermark — referința completă | https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html |
| **Learning Spark 2nd ed., cap. 8** `[FREE]` | Aceleași concepte, mai narativ | ebook Databricks |
| **Streaming Systems** — Akidau, Chernyak, Lax | Event-time, watermarking, exactly-once (avansat, conceptual) | carte O'Reilly |

**Mapare pe proiect:** explică `outputMode('complete')` vs `'append'`, `maxFilesPerTrigger`, `checkpointLocation` din C7.

---

## Football analytics & Expected Goals (xG) (adăugirea din C2)

| Resursă | Ce înțelegi | Unde |
|---|---|---|
| ⭐ **„Friends of Tracking" / Soccermatics** — David Sumpter (matematician) `[FREE]` | Cum se construiește un model xG real, de la zero | YouTube „Friends of Tracking" + cartea *Soccermatics* |
| **StatsBomb — articole despre xG** `[FREE]` | Ce e xG, de ce contează, cum se interpretează | statsbomb.com (blog) |

**Mapare pe proiect:** xG-ul empiric din C2 (rata de conversie per locație) e versiunea simplificată a modelelor explicate aici.

---

## „Drumul minim" — dacă ai doar un weekend

Ca să înțelegi ~80% din proiect, în ordinea asta:
1. **StatQuest** — Decision Trees + Random Forests + Gradient Boost + ROC&AUC (~1.5h) → acoperă C3, C4, C5
2. **3Blue1Brown** — Neural Networks ep. 1-2 (~40 min) → acoperă C6
3. **Learning Spark 2nd ed.** — cap. 3 (DataFrame) + cap. 8 (Streaming) (~2h citit) → acoperă C2, C7
4. **Blog Databricks** — „Pandas UDFs for PySpark" (~20 min) → acoperă C5
5. **Structured Streaming Programming Guide** — secțiunile „Basic Concepts" + „Output Modes" (~30 min) → C7

> Restul (Catalyst internals, Parquet format, Streaming Systems, ISLR complet) e pentru aprofundare după ce ai prins imaginea de ansamblu.
