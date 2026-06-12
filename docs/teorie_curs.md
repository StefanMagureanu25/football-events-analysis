# Teorie Big Data — Rezumat curs + resurse suplimentare

> Fiecare secțiune are la final un bloc cu: **📄 paperul fundamental** (autori + an), **⚠️ o întrebare frecventă de examen** cu răspuns scurt, și **✅/❌ când folosești / când NU** tehnologia. Lista completă de papere e consolidată la final.

## 1. Concepte fundamentale Big Data

### Cei 3V (și 10V extinși)
- **Volume** — cantitate masivă de date (TB, PB, ZB)
- **Velocity** — viteza cu care datele sunt generate și procesate (batch vs. real-time)
- **Variety** — tipuri diverse: structurate, semi-structurate, nestructurate
- Extinși: **Veracity** (acuratețe), **Value** (valoare business), **Variability**, **Visualization**, **Viscosity**, **Virality**, **Validity**

### Data Pipeline
- Traseul complet al datelor: **Colectare → Rafinare → Stocare → Analiză → Livrare**
- **Batch processing** — procesare la momente planificate; potrivit pentru volume mari
- **Stream processing** — procesare aproape în timp real; declanșat de evenimente

> **📄 Paper original:** Doug Laney, *„3D Data Management: Controlling Data Volume, Velocity and Variety"* (META Group / Gartner, **2001**) — originea celor 3V. Pentru arhitecturile de pipeline: Nathan Marz, *Lambda Architecture* (**2011**).
>
> **⚠️ Întrebare de examen:** *„Care sunt cei 3V și dă un exemplu pentru fiecare din proiectul tău?"* — **Răspuns:** Volume (941K evenimente), Velocity (simulăm streaming la Cerința 7), Variety (șuturi, corneruri, cartonașe — evenimente eterogene în același tabel).
>
> **✅ Când / ❌ când NU:** folosești **batch** când ai nevoie de throughput pe volume mari și latența nu contează (rapoarte nocturne); **stream** când ai nevoie de rezultate în secunde/minute. ❌ NU complica cu streaming când un job batch programat e suficient.

---

## 2. Hadoop & MapReduce (Curs 2-3)

### HDFS (Hadoop Distributed File System)
- Sistem de fișiere distribuit, fault-tolerant
- **NameNode** (Master) — păstrează metadata, coordonează
- **DataNode** (Slave) — stochează blocuri de date (~128 MB)
- Replicare implicită: **factor 3**

### MapReduce
- Paradigmă de procesare distribuită în **două faze**:
  1. **Map** — procesează fiecare înregistrare și emite perechi `(cheie, valoare)`
  2. **Reduce** — agregă toate valorile pentru aceeași cheie
- **Shuffle & Sort** — faza intermediară ce redistribuie datele între Map și Reduce
- Execuție pe disk (lentă la iterații multiple — dezavantaj față de Spark)

> **📄 Paper original:** Jeffrey Dean & Sanjay Ghemawat (Google), *„MapReduce: Simplified Data Processing on Large Clusters"* (**OSDI 2004**). HDFS se bazează pe *„The Google File System"* — Ghemawat, Gobioff, Leung (**SOSP 2003**); paperul HDFS direct: Shvachko et al., *„The Hadoop Distributed File System"* (**MSST 2010**).
>
> **⚠️ Întrebare de examen:** *„De ce e MapReduce lent pentru algoritmi iterativi (ex: ML)?"* — **Răspuns:** scrie rezultatele intermediare pe disc (HDFS) între fiecare fază; un algoritm iterativ reia I/O de disc la fiecare pas. Spark ține datele în memorie → 10-100× mai rapid pe iterativ.
>
> **✅ Când / ❌ când NU:** folosești Hadoop/MapReduce pentru job-uri batch masive, simple, one-pass, pe infrastructură Hadoop existentă. ❌ NU pentru iterativ (ML), interactiv sau low-latency — acolo Spark/Flink.

---

## 3. Apache Spark Core (Curs 3-5)

### Arhitectura Spark
```
Driver → SparkContext → Cluster Manager (YARN/Mesos/Kubernetes/Standalone)
                               ↓
                    Workers (Executors cu Tasks)
```

### RDD (Resilient Distributed Dataset)
- Abstractizarea de bază a Spark: colecție de date distribuită, imutabilă, fault-tolerant
- **Transformări (lazy)**: `map`, `filter`, `flatMap`, `join`, `groupByKey`, `reduceByKey`
- **Acțiuni (eager)**: `count`, `collect`, `reduce`, `saveAsTextFile`, `take`
- **Lineage graph (DAG)** — Spark reconstruiește date pierdute reluând transformările

### Dependențe
- **Narrow dependencies** — fiecare partiție input → o partiție output (map, filter)
- **Wide dependencies / Shuffle** — redistribuire date între noduri (groupByKey, join)

### Avantaje față de Hadoop
- **In-memory computing** — 10-100x mai rapid pentru algoritmi iterativi
- API unificat pentru batch, streaming, ML, grafuri

> **📄 Paper original:** Matei Zaharia et al. (UC Berkeley AMPLab), *„Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing"* (**NSDI 2012**). Precursor: *„Spark: Cluster Computing with Working Sets"* (**HotCloud 2010**).
>
> **⚠️ Întrebare de examen:** *„Cum recuperează Spark o partiție pierdută fără să replice datele ca HDFS?"* — **Răspuns:** prin **lineage** (DAG-ul de transformări): reaplică transformările deterministe care au produs partiția, pornind de la sursa de date. Nu replică, ci recalculează.
>
> **✅ Când / ❌ când NU:** folosești RDD direct când ai nevoie de control fin pe partiționare sau operații custom non-tabulare. ❌ NU pentru date tabulare/SQL — acolo DataFrame, fiindcă Catalyst optimizează, iar RDD nu.

---

## 4. Spark SQL (Curs 3-5)

### DataFrame API
- Colecție distribuită organizată în coloane cu schemă (ca un tabel SQL sau Pandas DataFrame)
- Optimizat prin **Catalyst Optimizer** (plan logic → plan fizic)
- **Tungsten** — execuție eficientă la nivel de memorie și CPU

### SparkSession
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("date.csv", header=True, inferSchema=True)
```

### Operații principale
```python
df.select("col1", "col2")          # proiecție
df.filter(df["col"] > 0)           # filtrare
df.groupBy("col").count()          # grupare
df.join(df2, "id", "inner")        # join
```

### Spark SQL (temp views)
```python
df.createOrReplaceTempView("events")
result = spark.sql("SELECT col1, COUNT(*) FROM events GROUP BY col1")
```

### UDF (User Defined Functions)
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def my_func(val):
    return "high" if val > 50 else "low"

df = df.withColumn("level", my_func(df["score"]))
```

### Dataset vs DataFrame
- **DataFrame** = Dataset[Row] — fără tip la compile time (Python/R)
- **Dataset** — cu tip la compile time (Scala/Java)

> **📄 Paper original:** Michael Armbrust et al. (Databricks), *„Spark SQL: Relational Data Processing in Spark"* (**SIGMOD 2015**) — introduce **Catalyst** (optimizator extensibil bazat pe reguli + cost) și integrarea cu Tungsten (code generation).
>
> **⚠️ Întrebare de examen:** *„De ce DataFrame e mai rapid decât RDD pentru aceeași operație?"* — **Răspuns:** DataFrame trece prin Catalyst (predicate pushdown, column pruning, reordonare join-uri) + codegen Tungsten; RDD e o cutie neagră pe care Spark nu o poate optimiza.
>
> **✅ Când / ❌ când NU (UDF):** folosești UDF doar când logica nu se poate exprima cu funcții native Spark. ❌ NU abuza — UDF e black-box pentru Catalyst și lentă (serializare rând-cu-rând). Preferă funcții native sau `pandas_udf` (vezi Cerința 5).

---

## 5. Spark Streaming (Curs 6)

### Data Pipeline
- **Collect → Refine → Store → Analyze → Deliver**
- KPI importanți: versioning, latență, scalabilitate, monitorizare, testare

### Structured Streaming
- Construit **peste Spark SQL** — folosește DataFrame API
- Modelul "**Unbounded Table**": fiecare înregistrare nouă = rând nou în tabelul de intrare
- Motor de procesare: **micro-batch** (latență ~100ms, exactly-once)
- **Continuous Processing** (Spark 2.3+): latență ~1ms, at-least-once

```python
inputDF = spark.readStream.json("s3://logs")
query = inputDF.groupBy("action", window("time", "1 hour")).count() \
               .writeStream.format("jdbc").start("jdbc:mysql://...")
```

### Spark Streaming (DStream API — legacy)
- Construit pe **RDD-uri** — Discretized Streams
- `DStream` = secvență continuă de RDD-uri, fiecare conținând date dintr-un interval
- `StreamingContext`, `socketTextStream`, `reduceByKey`
- Mai potrivit pentru batch processing decât real-time pur

### Diferențe cheie
| | Structured Streaming | Spark Streaming |
|---|---|---|
| API | DataFrame | DStream (RDD) |
| Real-time | Mai apropiat de real-time | Micro-batch |
| Event-time | Suport nativ | Limitat |
| Garanții | Exactly-once | At-least-once |

> **📄 Paper original:** DStreams — Zaharia et al., *„Discretized Streams: Fault-Tolerant Streaming Computation at Scale"* (**SOSP 2013**). Structured Streaming — Armbrust et al., *„Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark"* (**SIGMOD 2018**). Fundamentele event-time: Akidau et al., *„The Dataflow Model"* (**VLDB 2015**).
>
> **⚠️ Întrebare de examen:** *„Diferența între processing-time și event-time și de ce e nevoie de watermarking?"* — **Răspuns:** processing-time = când Spark vede evenimentul; event-time = când s-a produs efectiv. **Watermark** = cât întârziere accepți pentru evenimente sosite târziu înainte să închizi definitiv o fereastră de agregare (compromis latență vs corectitudine).
>
> **✅ Când / ❌ când NU:** Structured Streaming (DataFrame) pentru proiecte noi; DStream e legacy. Micro-batch când latența de ~secunde e acceptabilă; continuous processing pentru ~ms (dar at-least-once, experimental). ❌ NU folosi `outputMode("append")` pentru agregări fără watermark — Spark nu poate ști când fereastra e finală.

---

## 6. Apache Spark MLlib (Curs 7)

### Prezentare generală
- Biblioteca scalabilă de ML a lui Apache Spark
- **spark.ml** (DataFrame API, recomandat) vs **spark.mllib** (RDD API, legacy)
- Interoperează cu NumPy; suportă surse HDFS, Cassandra, Hive

### Algoritmi disponibili
| Categorie | Algoritmi |
|---|---|
| Clasificare | Regresie logistică, Naive Bayes, Random Forest, GBT, SVM |
| Regresie | Regresie liniară, Random Forest Regressor, GBT Regressor |
| Clustering | K-Means, GMM (Gaussian Mixture Models) |
| Recomandare | ALS (Alternating Least Squares) |
| Reducere dimensiune | PCA |
| Topic modeling | LDA (Latent Dirichlet Allocation) |

### Pipeline MLlib
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier

indexer = StringIndexer(inputCol="category", outputCol="category_idx")
assembler = VectorAssembler(inputCols=["f1", "f2", "category_idx"], outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[indexer, assembler, rf])
model = pipeline.fit(train_df)
```

### Hyperparameter Tuning
```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100]) \
    .addGrid(rf.maxDepth, [5, 10]) \
    .build()

cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid,
                    evaluator=BinaryClassificationEvaluator(), numFolds=3)
cvModel = cv.fit(train_df)
```

### Workflow ML
1. **Recuperare date** — surse interne/externe
2. **Pregătire** — curățare, explorare, transformare
3. **Dezvoltarea modelului** — alegere algoritm, antrenare
4. **Evaluare** — metrici (Accuracy, F1, AUC-ROC, RMSE)
5. **Desfășurare** — deployment în producție
6. **Monitorizare** — reantrenare cu date noi

### Scikit-learn vs MLlib
| | Scikit-learn | MLlib |
|---|---|---|
| Procesare | In-memory, single node | Distribuită, multi-node |
| Potrivit pentru | Date < RAM (~GB) | Date mari (zeci de GB+) |
| Vizualizare | Excelentă (Pandas, Matplotlib) | Mai limitată |
| Integrare Spark | Nu | Nativă |

> **📄 Papere originale:** MLlib — Meng et al., *„MLlib: Machine Learning in Apache Spark"* (**JMLR 2016**). Random Forest — Leo Breiman, *„Random Forests"* (**2001**). Gradient Boosting — Jerome Friedman, *„Greedy Function Approximation: A Gradient Boosting Machine"* (**Annals of Statistics, 2001**).
>
> **⚠️ Întrebare de examen:** *„Random Forest vs Gradient Boosting — care e diferența și care overfittează mai ușor?"* — **Răspuns:** RF = **bagging** (arbori în paralel, reduce varianța, greu de overfit crescând `numTrees`); GBT = **boosting** (arbori secvențiali care corectează erorile, reduce bias-ul, **overfittează mai ușor** dacă `maxIter`/learning rate sunt prea mari).
>
> **✅ Când / ❌ când NU:** MLlib când datele NU încap în RAM-ul unei mașini (procesare distribuită). ❌ NU pentru date mici (sub câțiva GB) — scikit-learn e mai rapid, mai bogat și fără overhead-ul Spark.

---

## 7. TensorFlow & Deep Learning

### Arhitectura rețelelor neuronale
- **Input Layer** → **Hidden Layers** (ReLU) → **Output Layer** (Sigmoid/Softmax)
- **Backpropagation** + **Gradient Descent** pentru optimizare
- **Regularizare**: Dropout, L1/L2

### TensorFlow cu Keras
```python
import tensorflow as tf

model = tf.keras.Sequential([
    tf.keras.layers.Dense(128, activation='relu', input_shape=(n_features,)),
    tf.keras.layers.Dropout(0.3),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(1, activation='sigmoid')  # clasificare binară
])

model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
model.fit(X_train, y_train, epochs=20, batch_size=256, validation_split=0.2)
```

> **📄 Papere originale:** TensorFlow — Abadi et al. (Google Brain), *„TensorFlow: A System for Large-Scale Machine Learning"* (**OSDI 2016**). Backpropagation — Rumelhart, Hinton, Williams, *„Learning representations by back-propagating errors"* (**Nature 1986**). Optimizatorul **Adam** — Kingma & Ba (**ICLR 2015**). **Dropout** — Srivastava, Hinton et al. (**JMLR 2014**). **Batch Normalization** — Ioffe & Szegedy (**2015**).
>
> **⚠️ Întrebare de examen:** *„De ce normalizezi input-ul pentru o rețea neurală, dar nu pentru Random Forest?"* — **Răspuns:** gradient descent converge prost cu features pe scale diferite (gradienți dezechilibrați, oscilații); normalizarea le aduce la scală comparabilă. Arborii (RF/GBT) compară praguri per feature → invarianți la scală.
>
> **✅ Când / ❌ când NU:** Deep Learning când ai **multe** date și relații non-liniare complexe (imagini, text, secvențe, audio). ❌ NU pentru date tabulare mici/medii — acolo gradient boosting (XGBoost/LightGBM) bate de obicei rețelele, cu mai puțin efort de tuning.

---

## 8. Feature Engineering

### Tipuri de caracteristici
- **Cantitative**: Discrete (număr goluri), Continue (cotă meci)
- **Calitative**: Nominale (liga, echipă), Ordinale (locație șut)

### Tehnici importante
- **One-Hot Encoding** — variabile categorice → vectori binari
- **Normalizare / Standardizare** — scalarea valorilor numerice
- **PCA** — reducerea dimensiunilor
- **Selecția atributelor** — eliminarea coloanelor redundante

> **📄 Referință:** PCA — Karl Pearson (**1901**) / Harold Hotelling (**1933**). Carte de referință modernă: Max Kuhn & Kjell Johnson, *„Feature Engineering and Selection"* (**2019**).
>
> **⚠️ Întrebare de examen:** *„Când folosești One-Hot Encoding vs Label/Index Encoding?"* — **Răspuns:** One-Hot pentru categorii **nominale** fără ordine (evită ordine falsă), dar explodează dimensionalitatea la cardinalitate mare; Index/Label encoding pentru **ordinale** sau pentru modele bazate pe arbori (RF/GBT) care nu presupun ordine liniară între coduri.
>
> **✅ Când / ❌ când NU:** normalizare/standardizare obligatorie pentru modele bazate pe distanță/gradient (NN, KNN, SVM, regresie liniară); ❌ inutilă pentru arbori (RF/GBT) — sunt invarianți la transformări monotone de scală.

---

## Papere fundamentale (listă consolidată pentru studiu)

| # | Tehnologie | Paper | Autori | An |
|---|---|---|---|---|
| 1 | 3V Big Data | 3D Data Management | Doug Laney (Gartner) | 2001 |
| 2 | MapReduce | MapReduce: Simplified Data Processing... | Dean & Ghemawat (Google) | 2004 |
| 3 | GFS/HDFS | The Google File System | Ghemawat, Gobioff, Leung | 2003 |
| 4 | Spark/RDD | Resilient Distributed Datasets | Zaharia et al. (Berkeley) | 2012 |
| 5 | Spark SQL | Spark SQL: Relational Data Processing | Armbrust et al. (Databricks) | 2015 |
| 6 | DStreams | Discretized Streams | Zaharia et al. | 2013 |
| 7 | Structured Streaming | A Declarative API for Real-Time Apps | Armbrust et al. | 2018 |
| 8 | Dataflow/event-time | The Dataflow Model | Akidau et al. (Google) | 2015 |
| 9 | MLlib | MLlib: Machine Learning in Apache Spark | Meng et al. | 2016 |
| 10 | Random Forest | Random Forests | Leo Breiman | 2001 |
| 11 | Gradient Boosting | Greedy Function Approximation | Jerome Friedman | 2001 |
| 12 | TensorFlow | A System for Large-Scale ML | Abadi et al. (Google) | 2016 |
| 13 | Backpropagation | Learning representations by back-prop | Rumelhart, Hinton, Williams | 1986 |
| 14 | Adam | Adam: A Method for Stochastic Optimization | Kingma & Ba | 2015 |
| 15 | Dropout | Dropout: A Simple Way to Prevent Overfitting | Srivastava et al. | 2014 |
| 16 | Batch Norm | Batch Normalization | Ioffe & Szegedy | 2015 |

---

## Resurse suplimentare recomandate

### Cărți
| Carte | Autor | Relevanță |
|---|---|---|
| *Learning Spark* (2nd ed., 2020) | Jules Damji et al. | Spark SQL, Streaming, MLlib |
| *High Performance Spark* | Holden Karau, Rachel Warren | Optimizare, performanță |
| *Hands-On Machine Learning* (3rd ed.) | Aurélien Géron | ML/DL practic cu sklearn + TF |
| *Deep Learning* | Goodfellow, Bengio, Courville | Teorie DL profundă |
| *Streaming Systems* | Tyler Akidau et al. | Fundamente procesare stream |
| *Designing Data-Intensive Applications* | Martin Kleppmann | Sisteme de date (must-read pentru backend→DE) |

### Cursuri online
| Platformă | Curs |
|---|---|
| Coursera | *Big Data Specialization* — UC San Diego |
| Databricks Academy | Apache Spark fundamentals (gratuit) |
| fast.ai | Practical Deep Learning for Coders |
| DeepLearning.AI | TensorFlow Developer Certificate |

### Documentație oficială
- Apache Spark: https://spark.apache.org/docs/latest/
- Spark MLlib: https://spark.apache.org/docs/latest/ml-guide.html
- Spark Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- TensorFlow: https://www.tensorflow.org/guide
- Keras: https://keras.io/guides/

### Articole & bloguri
- Databricks Blog: https://www.databricks.com/blog
- Towards Data Science (Medium): articole practice PySpark + ML
- Papers With Code: https://paperswithcode.com — state-of-the-art în ML/DL

### Dataset-uri pentru practică suplimentară
- Kaggle: https://www.kaggle.com/datasets
- UCI ML Repository: https://archive.ics.uci.edu/
- Google Dataset Search: https://datasetsearch.research.google.com/
