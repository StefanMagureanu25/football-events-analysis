# Cerințe Proiect Big Data — Football Events Analysis

**Dataset:** Football Events (Kaggle) — `events.csv` (941.009 rânduri) + `ginf.csv` (10.112 rânduri)
**Sursă:** https://www.kaggle.com/datasets/secareanualin/football-events
**Format livrabil:** Jupyter Notebook (.ipynb) cu output-uri salvate
**Notare:** 1 oficiu + N1 (max 4p, cerințele 1-4) + N2 (max 2p, cerințele 5-7) + N3 (max 3p, complexitate)

> Acest document descrie, pentru fiecare cerință: **(1) ce face concret implementarea**, **(2) cum funcționează intern** tehnologia folosită (nu doar ce API am apelat) și **(3) „Ce ar trebui să știi la prezentare"** — întrebări tehnice probabile cu răspunsuri scurte.
>
> Notele sunt scrise cu analogii din backend engineering acolo unde ajută înțelegerea.

---

## Scopul proiectului

**Întrebarea centrală:** *poate fi prezis golul din caracteristicile unui șut — și ce echipe marchează mai mult decât ar „merita" statistic?*

Proiectul construiește un **sistem end-to-end de analiză și predicție** peste 941.009 evenimente reale de fotbal (5 ligi europene, sezoanele 2012–2017), parcurgând întregul ciclu de viață al unei aplicații de date — exact etapele pe care le-ar parcurge o echipă de data engineering într-o companie:

1. **Înțelegerea datelor** (C2) — analiză exploratorie distribuită: unde se marchează, cine convertește, ce atribute poartă semnal. Aici se construiește și un model **xG (expected goals)** empiric — metrica folosită azi de cluburi profesioniste și case de pariuri pentru a măsura calitatea ocaziilor independent de rezultat.
2. **Modelare predictivă** (C3, C5, C6) — trei abordări pe aceeași problemă: *„șutul acesta devine gol?"* (Random Forest, Random Forest optimizat cu CrossValidator, rețea neuronală TensorFlow) plus o problemă de regresie (*câte goluri va avea meciul?*). Compararea lor arată trade-off-urile reale dintre metode.
3. **Industrializare** (C4) — datele curățate devin un artefact reproductibil (Parquet), iar modelul devine un pipeline serializat care poate fi livrat — granița dintre „experiment în notebook" și „sistem de producție".
4. **Operare în timp real** (C7) — modelul antrenat offline e aplicat pe un flux live de evenimente, simulând exact scenariul real: predicții per șut, în timpul meciului, pe măsură ce datele sosesc.

**Scopul didactic** (de spus la prezentare dacă ești întrebat „de ce acest proiect?"): demonstrarea întregului ecosistem Big Data — Spark SQL + DataFrame API pentru procesare distribuită, MLlib pentru ML scalabil, TensorFlow pentru deep learning, Structured Streaming pentru timp real — pe un caz de utilizare unitar și real, nu pe exemple izolate. Firul roșu care leagă toate cele 7 cerințe este predicția golului: aceeași țintă (`is_goal`), atacată cu instrumente din ce în ce mai sofisticate, până la inferență live.

---

## Ordinea de rulare a notebook-urilor

Există dependențe între notebook-uri (artefacte pe disc):

```
Cerința 2  (EDA, independent)
Cerința 4  →  scrie  data/processed_shots/ (Parquet)  +  models/rf_pipeline_model/
Cerința 3  (folosește events.csv direct)
Cerința 5  →  citește data/processed_shots/ (Parquet de la C4)
Cerința 6  →  scrie  models/tf_goal_predictor.keras  +  models/tf_scaler.joblib
Cerința 7  →  încarcă models/rf_pipeline_model/ (de la C4)
```

**Ordine recomandată:** `2 → 4 → 3 → 5 → 6 → 7`. Cerința 4 trebuie rulată înaintea lui 5 și 7.

---

## Cerința 1 — Introducere *(parte din N1)*

### Ce face implementarea
Notebook-ul `Proiect_BigData.ipynb`: pagină de titlu, cuprins, descrierea celor două fișiere, link obligatoriu către dataset, enunțarea celor 6 obiective (O1–O6 mapate pe cerințele 2–7). Celule pandas care afișează un preview al datelor și dimensiunile.

### Cum funcționează intern (conceptele de bază)
- **De ce „Big Data" aici:** 941K evenimente nu încap confortabil într-o singură mașină dacă faci join-uri și agregări repetate în memorie — Spark partiționează datele și execută distribuit. Analogie backend: e diferența dintre a procesa o listă în RAM și a rula un query pe un cluster de DB-uri sharded.
- **Cele 3V (Volume, Velocity, Variety):** volumul (~1M rânduri), velocity (simulăm streaming la Cerința 7), variety (evenimente eterogene: șuturi, corneruri, cartonașe în același tabel).

### Ce ar trebui să știi la prezentare
1. **Ce conține dataset-ul?** `events.csv` = evenimente granulare per meci (event_type codifică tipul: 1=șut, 2=corner, 3=fault etc.), `ginf.csv` = metadate per meci (echipe, scor final `fthg`/`ftag`, ligă, sezon, cote). Cheia de join e `id_odsp`.
2. **De ce e o problemă de Big Data și nu un simplu pandas?** Volumul + nevoia de procesare distribuită, join-uri pe ~1M rânduri, și pentru că proiectul cere demonstrarea ecosistemului Spark (SQL, MLlib, Streaming).
3. **Care e ținta principală de predicție?** `is_goal` (clasificare binară: șutul e gol sau nu) și `total_goals` per meci (regresie).
4. **Care e dezechilibrul de clase?** Doar ~10-12% din șuturi sunt goluri — relevant pentru toate modelele (tratat cu `weightCol` / `class_weight`).

---

## Cerința 2 — Spark SQL + DataFrame API *(N1)*

### Ce face implementarea
`Cod_sursa_cerinta_2.ipynb`. Inițializează `SparkSession`, citește ambele CSV-uri cu `inferSchema`, verifică null-uri, filtrează tentativele de șut (`event_type == 1`). Analize, **folosind alternativ ambele interfețe**:
- **Spark SQL** (temp views `events`, `matches`, `shots`): distribuția tipurilor de evenimente, rata de conversie pe zonă de teren, statistici per ligă, conversie pe situație×parte corp (heatmap).
- **DataFrame API**: top echipe după goluri, distribuția golurilor pe intervale de 5 min, top jucători (join `events`×`ginf`).
- **Adăugiri (complexitate, N3):**
  - **xG estimat** (secțiunea 2.10, Spark SQL cu CTE-uri): fiecare locație primește o probabilitate empirică de gol; însumate pe meci dau „expected goals", comparate cu golurile reale → echipe care supra/sub-performează.
  - **Matrice de corelație** (2.11, `pyspark.ml.stat.Correlation`): corelația Pearson între atributele șuturilor, calculată distribuit pe un vector de features.
  - **Evoluția golurilor pe sezoane/ligi** (2.12, DataFrame API + `pivot_table`): serie temporală, o linie per ligă.

### Cum funcționează intern
- **Lazy evaluation + DAG:** transformările (`filter`, `groupBy`, `select`) nu se execută imediat — Spark construiește un **graf de execuție** (DAG) și îl rulează doar la o **acțiune** (`show`, `count`, `collect`, `toPandas`). Analogie backend: ca un query builder ORM care nu trimite SQL până nu faci `.all()`.
- **Catalyst Optimizer:** atât SQL cât și DataFrame API trec prin același optimizator (Catalyst) care rescrie planul logic (predicate pushdown, column pruning, reordonare join-uri) într-un plan fizic optim. De aceea SQL și DataFrame API au **aceeași performanță** — sunt doar două front-end-uri peste același motor. Analogie: query planner-ul din PostgreSQL.
- **Temp views** (`createOrReplaceTempView`) nu copiază datele — sunt doar un nume logic pentru DataFrame, ca un `CREATE VIEW` (nu materializat).
- **`spark.sql.shuffle.partitions=8`:** la operații cu shuffle (`groupBy`, `join`), Spark redistribuie datele în N partiții. Default e 200 — mult prea mult pentru un dataset local, deci l-am redus la 8.
- **Narrow vs wide transformations:** `filter`/`withColumn` = narrow (fiecare partiție independentă, fără mișcare de date); `groupBy`/`join` = wide (necesită **shuffle** = mutare date peste rețea între executori). Shuffle-ul e operația scumpă.

### Ce ar trebui să știi la prezentare
1. **Care e diferența între DataFrame API și Spark SQL?** Niciuna la nivel de performanță — ambele compilează prin Catalyst la același plan fizic. SQL e mai natural pentru agregări declarative, DataFrame API pentru transformări programatice.
2. **Ce e lazy evaluation și de ce contează?** Spark amână execuția până la o acțiune, ca să poată optimiza tot lanțul deodată (ex: împinge filtrul înainte de join). Fără el, fiecare pas s-ar materializa inutil.
3. **Ce face un `groupBy` „sub capotă"?** Declanșează un **shuffle**: datele cu aceeași cheie sunt mutate pe același executor, apoi agregate. E motivul pentru care `groupBy` pe chei cu cardinalitate mare e lent.
4. **De ce `inferSchema=True` e costisitor?** Spark face o **trecere suplimentară** peste tot fișierul ca să deducă tipurile. În producție se dă schema explicit (cum facem la Cerința 7).
5. **Cum ai calculat xG fără un model antrenat?** Empiric: rata istorică de conversie per locație = P(gol|locație). E un lookup table calculat cu un CTE, apoi join înapoi pe șuturi. E un xG simplificat (doar pe locație).

---

## Cerința 3 — Minim 2 metode ML cu Spark MLlib *(N1)*

### Ce face implementarea
`Cod_sursa_cerinta_3.ipynb`. Două metode metodologic diferite:
- **Clasificare — `RandomForestClassifier`** (`numTrees=100`, `maxDepth=8`): prezice `is_goal` din 8 features (location, bodypart, situation, assist_method, fast_break, shot_place, side, time). Evaluare: AUC-ROC, Accuracy, F1, Precision, Recall (weighted), feature importance, matrice de confuzie.
- **Regresie — `GBTRegressor`** (`maxIter=50`, `maxDepth=5`, `stepSize=0.1`): prezice `total_goals` (= `fthg+ftag`) din statistici agregate per meci (șuturi, corneruri, fault-uri, cartonașe, penaltiuri, contraatacuri). Evaluare: RMSE, MAE, R².
- **Adăugiri (N3):**
  - **Diagnoză overfitting:** AUC pe train vs test + verdict automat pe baza gap-ului.
  - **Tratarea dezechilibrului (`weightCol`):** ponderi *balanced* (clasa minoritară = goluri primește pondere mai mare), comparație recall pe goluri baseline vs ponderat.

### Cum funcționează intern
- **Random Forest = bagging:** antrenează `numTrees` arbori de decizie **independenți**, fiecare pe un eșantion bootstrap (cu repunere) și pe un subset aleator de features la fiecare split. Predicția = **votul majoritar** (clasificare). Reduce varianța față de un singur arbore. Analogie: load balancing — multe instanțe independente, decizia finală e agregarea lor.
- **GBT = boosting:** antrenează arbori **secvențial**, fiecare arbore nou corectând erorile (reziduurile) celor anteriori, ponderat cu `stepSize` (learning rate). Reduce bias-ul. Diferența cheie față de RF: arborii nu sunt independenți, ci dependenți în lanț.
- **Cum alege un arbore un split:** caută feature-ul + pragul care **maximizează scăderea impurității** (Gini sau entropie pentru clasificare; varianță pentru regresie). E o căutare greedy, nod cu nod.
- **Feature importance:** cât a contribuit fiecare feature la scăderea totală de impuritate, mediat peste toți arborii.
- **AUC-ROC:** probabilitatea ca modelul să dea un scor mai mare unui exemplu pozitiv aleator decât unuia negativ aleator. 0.5 = aleator, 1.0 = perfect. Independent de prag.
- **`weightCol` intern:** fiecare exemplu intră în funcția de pierdere cu ponderea lui — un gol „greșit clasificat" e penalizat mai tare, forțând modelul să nu mai ignore minoritarul.

### Ce ar trebui să știi la prezentare
1. **Random Forest vs GBT — care e diferența fundamentală?** Bagging (arbori independenți, în paralel, reduce **varianța**) vs boosting (arbori secvențiali care corectează erorile, reduce **bias-ul**).
2. **De ce ai ales clasificare ȘI regresie?** Cerința cere ≥2 metode; am ales două probleme diferite (binară vs continuă) cu algoritmi diferiți, ca să demonstrez diversitate.
3. **Ce înseamnă AUC=0.76 (de ex.)?** Modelul ordonează corect un șut-gol înaintea unui șut-ne-gol în 76% din perechi. E peste aleator (0.5), rezonabil pentru predicția golului care e intrinsec incertă.
4. **Cum ai detectat overfitting-ul?** Comparând AUC train vs test: un gap mare (>0.05) = modelul memorează train-ul. La noi RF cu `maxDepth=8` are gap mic → generalizează.
5. **De ce e nevoie de `VectorAssembler`?** MLlib lucrează cu o singură coloană `features` de tip vector, nu cu coloane separate. Assembler-ul le concatenează — e un pas obligatoriu de „împachetare".
6. **Cum ai tratat dezechilibrul?** Cu `weightCol`: ponderi invers proporționale cu frecvența clasei. Crește recall-ul pe goluri, cu un mic compromis pe accuracy.

---

## Cerința 4 — Data Pipeline *(N1)*

### Ce face implementarea
`Cod_sursa_cerinta_4.ipynb`. Două pipeline-uri:
- **Pipeline ETL:** CSV brut → filtrare (șuturi cu date complete) → curățare null-uri (`fillna`) → cast tipuri → **feature engineering** (`is_central_shot`, `time_phase`, `is_set_piece`) → scriere **Parquet** partiționat după `situation` în `data/processed_shots/`. Verificare prin recitire.
- **Pipeline MLlib** (`pyspark.ml.Pipeline`): `Imputer(median)` → `VectorAssembler` → `StandardScaler(withMean, withStd)` → `RandomForestClassifier`. Antrenat, evaluat (AUC), **serializat** în `models/rf_pipeline_model/`, **reîncărcat** (`PipelineModel.load`) și re-evaluat pentru a demonstra reproductibilitatea (AUC identic).

### Cum funcționează intern
- **Parquet — format columnar:** stochează datele **pe coloane**, nu pe rânduri. Avantaje: (1) **compresie** mai bună (valori similare adiacente), (2) **column pruning** (citești doar coloanele cerute), (3) **predicate pushdown** (filtrele se aplică la citire). Analogie backend: ca un column-store (Cassandra/Redshift) vs un row-store (PostgreSQL clasic) — ideal pentru analytics, nu pentru OLTP.
- **`partitionBy('situation')`:** scrie datele în subdirectoare (`situation=1/`, `situation=2/`...). La citire cu filtru pe `situation`, Spark sare peste partițiile irelevante (**partition pruning**) — citește mai puțin de pe disc.
- **Estimator vs Transformer (concept central MLlib):** un **Transformer** are doar `.transform()` (ex: VectorAssembler — aplică o regulă fixă). Un **Estimator** are `.fit()` care **învață** ceva din date și produce un Transformer (ex: `StandardScaler.fit()` învață media/deviația → `StandardScalerModel`). `Pipeline.fit()` rulează `fit` pe estimatori în ordine și `transform` pe transformeri.
- **De ce serializăm pipeline-ul întreg, nu doar modelul:** la inferență trebuie aplicate **exact aceleași transformări** (aceeași mediană de imputare, aceeași medie/deviație de scalare) învățate pe train. Salvarea pipeline-ului întreg garantează asta. Analogie: ca un container Docker — împachetezi tot lanțul, nu doar binarul final.

### Ce ar trebui să știi la prezentare
1. **Ce e Parquet și de ce e mai bun decât CSV aici?** Format columnar comprimat → citire mai rapidă, compresie, column pruning, predicate pushdown. CSV e text plat, fără tipuri, fără compresie.
2. **Ce face `partitionBy` și când ajută?** Împarte fizic datele în foldere după valoarea coloanei. Ajută când filtrezi des pe acea coloană (partition pruning). Atenție la cardinalitate mare (prea multe fișiere mici).
3. **Diferența Estimator vs Transformer?** Estimator `.fit()` → învață din date → produce un model (Transformer); Transformer `.transform()` → aplică. Pipeline-ul orchestrează ambele.
4. **De ce `StandardScaler` în pipeline dacă RF nu are nevoie de scalare?** Corect — RF e invariant la scală; l-am inclus ca să demonstrez un pipeline complet și reutilizabil (e esențial pentru modele sensibile la scală, ca rețeaua de la C6).
5. **Ce se salvează când faci `pipeline_model.save()`?** Toți parametrii învățați ai fiecărei etape (mediana imputerului, media/deviația scaler-ului, toți arborii RF) + metadatele pipeline-ului. La `load` obții exact același comportament.
6. **De ce reîncarci și re-evaluezi?** Ca să dovedești reproductibilitatea — AUC identic înainte și după serializare confirmă că artefactul de pe disc e complet.

---

## Cerința 5 — UDF + Optimizarea hiperparametrilor *(N2)*

### Ce face implementarea
`Cod_sursa_cerinta_5.ipynb`.
- **UDF standard** (`@udf`): `shot_quality_score(location, bodypart, situation, fast_break) → Float[0,1]` (scor compozit ponderat) + `shot_quality_label(score) → String` (excelent/bun/mediu/slab). Înregistrate și în Spark SQL; analiză a ratei de conversie pe categorii de calitate.
- **Adăugire (N3): Pandas UDF vectorizat** (`@pandas_udf`): aceeași logică, dar pe batch-uri (pandas Series) prin Apache Arrow. **Verificare de echivalență** (diff max ≈ 0) + **benchmark** de timp UDF vs Pandas UDF (cu warmup pe ambele căi).
- **CrossValidator + ParamGridBuilder:** grid de `numTrees [50,100]` × `maxDepth [5,10]` × `minInstancesPerNode [1,5]` (8 combinații), `numFolds=3`, `parallelism=2`, pe un sample de 30%. Afișează cea mai bună combinație + AUC per combinație.

### Cum funcționează intern
- **De ce UDF standard e lent:** Spark trimite **fiecare rând individual** din JVM către un proces Python worker (serializare pickle), execută funcția Python interpretat, serializează rezultatul înapoi. Overhead masiv de serializare + Catalyst **nu poate optimiza** interiorul unei UDF (e o cutie neagră). Analogie backend: problema **N+1 query-uri**.
- **De ce Pandas UDF e rapid:** transferă **loturi întregi** de rânduri prin **Apache Arrow** (format columnar binar, zero-copy între JVM și Python), iar logica rulează **vectorizat** în NumPy (o singură operație pe tot batch-ul). Analogie: un singur batch query în loc de N apeluri.
- **k-fold Cross-Validation:** împarte train-ul în k=3 felii; antrenează pe k-1 felii, validează pe a treia, rotește. Scorul final = media celor k. Mai robust decât un singur split (nu te bazezi pe „norocul" unei singure împărțiri).
- **Combinatorica grilei:** 8 combinații × 3 folds = **24 de antrenări** de model. `parallelism=2` rulează 2 în paralel.

### Ce ar trebui să știi la prezentare
1. **De ce e o UDF mai lentă decât funcțiile native Spark?** Serializare rând-cu-rând JVM↔Python + Catalyst nu o poate optimiza (black box). Funcțiile native rulează direct în JVM, optimizate.
2. **Cum rezolvă Pandas UDF problema?** Procesează batch-uri prin Arrow (columnar, zero-copy) și vectorizat în NumPy → drastic mai puține round-trip-uri. Am măsurat speedup-ul în notebook.
3. **Ce e Apache Arrow și de ce contează?** Un format de memorie columnar standardizat care permite transfer fără copiere/serializare între sisteme (JVM ↔ Python). E „limbajul comun" care elimină costul de traducere.
4. **De ce k-fold CV și nu un singur train/test split?** Estimezi performanța mai robust (medie peste k împărțiri), reduci dependența de o singură partiționare norocoasă/nefastă. Esențial când dataset-ul nu e uriaș.
5. **Câte modele s-au antrenat la tuning?** 8 combinații × 3 folds = 24. De aceea am folosit un sample de 30% — altfel ar dura prea mult.
6. **De ce ai înregistrat UDF-ul și în SQL (`spark.udf.register`)?** Ca să-l pot apela direct în interogări SQL, nu doar în DataFrame API.

---

## Cerința 6 — Deep Learning cu TensorFlow *(N2)*

### Ce face implementarea
`Cod_sursa_cerinta_6.ipynb`. Citește datele cu Spark, le aduce în pandas (`toPandas`), split **stratificat** 70/15/15 (train/val/test), normalizare cu `StandardScaler`. Rețea **Keras Sequential**: `Dense(256) → BatchNorm → Dropout(0.4) → Dense(128) → BatchNorm → Dropout(0.3) → Dense(64) → Dropout(0.2) → Dense(1, sigmoid)`. `Adam(lr=0.001)`, `binary_crossentropy`, metrici accuracy/AUC/Precision/Recall. `class_weight` pentru dezechilibru, `EarlyStopping(monitor=val_auc)` + `ReduceLROnPlateau`. Evaluare pe test, curbe de antrenare, ROC + matrice de confuzie, comparație cu RF din C3.
- **Adăugiri (N3):** distribuția probabilităților prezise pe clase, **curba Precision-Recall** (mai relevantă decât ROC pe date dezechilibrate), salvarea scaler-ului (`joblib.dump` → `models/tf_scaler.joblib`).

### Cum funcționează intern
- **Forward + backpropagation:** la forward, input-ul trece prin straturi (înmulțire matrice + funcție de activare). Loss-ul (binary crossentropy) măsoară eroarea. La **backprop**, gradientul loss-ului se propagă înapoi prin chain rule, iar **Adam** actualizează ponderile în direcția care scade loss-ul.
- **Adam:** optimizator cu learning rate adaptiv per parametru (combină momentum + RMSProp). Converge mai repede și mai stabil decât SGD simplu.
- **BatchNormalization:** normalizează activările fiecărui batch (medie 0, varianță 1) → antrenare mai stabilă și mai rapidă, mai puțin sensibilă la inițializare.
- **Dropout:** dezactivează aleator un procent de neuroni la fiecare pas de antrenare → previne co-adaptarea, e o formă de **regularizare** (ca un ensemble implicit). La inferență e dezactivat.
- **Sigmoid + binary crossentropy:** sigmoid mapează output-ul la (0,1) = probabilitate; BCE penalizează puternic predicțiile încrezătoare dar greșite.
- **De ce scalăm features pentru NN dar nu pentru RF:** gradient descent converge prost când features-urile au scale foarte diferite (gradienți dezechilibrați). Arborii (RF) compară praguri per feature, deci sunt invarianți la scală.
- **`class_weight` intern:** înmulțește contribuția fiecărei clase la loss — clasa minoritară (goluri) cântărește mai mult, deci modelul nu o mai ignoră.

### Ce ar trebui să știi la prezentare
1. **De ce o rețea neurală și ce aduce în plus față de RF?** Poate învăța **interacțiuni non-liniare** complexe între features prin straturi ascunse. La date tabulare mici, diferența față de RF e adesea mică — de aceea le comparăm explicit.
2. **Ce fac BatchNorm și Dropout?** BatchNorm stabilizează antrenarea normalizând activările; Dropout previne overfitting-ul dezactivând aleator neuroni. Ambele combat overfitting-ul pe date dezechilibrate.
3. **De ce normalizezi datele aici dar nu la Random Forest?** NN cu gradient descent e sensibil la scala features (altfel gradienții explodează/dispar); arborii nu, fiindcă lucrează cu praguri.
4. **De ce Precision-Recall în loc de ROC?** Pe date dezechilibrate, ROC e dominat de true-negative rate (clasa majoritară) și pare optimist. PR se concentrează pe clasa pozitivă (goluri) → mai onest.
5. **Ce face `class_weight` și de ce 1:~9?** Compensează raportul ~10% goluri: penalizează de ~9× mai tare erorile pe goluri în funcția de pierdere.
6. **De ce salvezi scaler-ul separat (`joblib`)?** La inferență datele noi trebuie scalate cu **exact** media/deviația din train. Fără scaler salvat, modelul primește input pe altă scară → predicții greșite.
7. **Ce face EarlyStopping?** Oprește antrenarea când metrica de validare (val_auc) nu se mai îmbunătățește de `patience` epoci și restaurează cele mai bune ponderi → evită overfitting-ul și economisește timp.

---

## Cerința 7 — Spark Structured Streaming + inferență în timp real *(N2)*

### Ce face implementarea
`Cod_sursa_cerinta_7.ipynb`. Simulează un flux live de la un meci: împarte șuturi reale în batch-uri scrise treptat ca fișiere CSV într-un director monitorizat. **Schema explicită** (`StructType`), `readStream` cu `maxFilesPerTrigger=1`, `trigger(processingTime='5 seconds')`.
- **Query 1** — statistici per echipă (șuturi, goluri, rată conversie), `outputMode='complete'`.
- **Query 2** — predicții individuale pe fiecare șut, `outputMode='append'` + `checkpointLocation`, aplicând **pipeline-ul MLlib încărcat de la C4** (`PipelineModel.load`) pe stream (după ce reconstruiește features-urile inginerite `is_central_shot`, `time_phase`, `is_set_piece`).
- **Query 3** — procesare batch finală a tuturor datelor pentru validare.

### Cum funcționează intern
- **Micro-batch:** Structured Streaming nu e „event-by-event" real, ci execută **micro-batch-uri** periodice (la fiecare trigger). Tratează stream-ul ca o **tabelă infinită** care crește; fiecare trigger procesează rândurile noi. Analogie backend: un cron job care procesează incremental ce e nou de la ultima rulare.
- **Output modes:**
  - `complete` — rescrie **tot** rezultatul la fiecare trigger (necesar pentru agregări globale, ex: total goluri per echipă, care se pot schimba retroactiv).
  - `append` — scrie **doar rândurile noi** (potrivit când un rând o dată finalizat nu se mai schimbă, ex: o predicție per șut).
  - `update` — scrie doar rândurile modificate.
- **Checkpointing:** salvează **offset-urile** procesate + starea agregărilor într-un director. La restart, Spark reia de unde a rămas → **exactly-once** și toleranță la defecte. Analogie backend: commit-ul de offset în Kafka / un Write-Ahead Log.
- **De ce schema explicită e obligatorie la file streams:** Spark nu poate infera schema dintr-un fișier care încă nu există la pornirea stream-ului — trebuie să o știe dinainte ca să interpreteze fișierele noi consistent.
- **Inferență offline→online (Lambda architecture):** modelul e antrenat offline (batch, C4) și aplicat online pe stream. Același `PipelineModel` rulează în ambele contexte — un argument puternic pentru serializarea pipeline-ului întreg.

### Ce ar trebui să știi la prezentare
1. **Structured Streaming e procesare în timp real „adevărată"?** Nu — e **micro-batch** (loturi mici, frecvente). Tratează stream-ul ca o tabelă care crește la infinit. (Există și modul continuous, experimental.)
2. **Care e diferența între output modes și când folosești fiecare?** `complete` pentru agregări care se pot schimba (rescrie tot); `append` pentru rânduri finale imuabile (predicții); `update` pentru doar ce s-a modificat. Agregările nu merg cu `append` fără watermark.
3. **La ce servește checkpoint-ul?** Persistă offset-urile și starea → la restart reia exact de unde a rămas (exactly-once), toleranță la defecte. E echivalentul commit-ului de offset din Kafka.
4. **De ce trebuie schema dată explicit?** Fișierele din stream nu există la pornire, deci Spark nu le poate infera schema — o dai dinainte pentru consistență.
5. **Cum aplici modelul ML pe stream?** Încarc `PipelineModel` antrenat offline (C4) și apelez `.transform()` pe DataFrame-ul de streaming — exact ca pe unul batch. Asta arată integrarea offline→online.
6. **De ce reconstruiești `is_central_shot`, `time_phase`, `is_set_piece` în stream?** Pentru că pipeline-ul de la C4 a fost antrenat cu aceste features inginerite — datele de stream trebuie aduse la **exact aceeași schemă de features** ca la antrenare.

---

## Ce dă punctele de complexitate (N3, max 3p)

Elemente peste minimul cerut, de evidențiat la prezentare:
- **xG model** empiric din locație (C2) + **matrice de corelație distribuită** (MLlib `Correlation`) + serie temporală pivotată.
- **Tratarea dezechilibrului** cu `weightCol` + **diagnoză overfitting** train vs test (C3).
- **Pandas UDF vectorizat + benchmark** UDF vs Arrow (C5) — demonstrează înțelegerea costului de serializare.
- **CrossValidator** cu grid de 24 antrenări (C5).
- **Pipeline ETL → Parquet partiționat** + **Pipeline MLlib serializat și reîncărcat** (C4).
- **Precision-Recall + distribuția probabilităților** pe date dezechilibrate (C6) + scaler serializat.
- **Streaming cu inferență ML** offline→online, 3 query-uri, 2 output modes, checkpointing (C7).

---

## Structura fișierelor de livrat

```
football-events-analysis/
├── notebooks/                      # cele 7 notebook-uri (cod sursă)
│   ├── 505_Magureanu_Stefan_Ionut-Proiect_BigData.ipynb       # Cerința 1
│   ├── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_2.ipynb
│   ├── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_3.ipynb
│   ├── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_4.ipynb
│   ├── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_5.ipynb
│   ├── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_6.ipynb
│   └── 505_Magureanu_Stefan_Ionut-Cod_sursa_cerinta_7.ipynb
├── docs/                           # documentație
│   ├── cerinte_proiect.md
│   ├── teorie_curs.md
│   ├── resurse_proiect.md
│   ├── LEARNING_ROADMAP.md
│   ├── PROMPT_OPUS.md
│   └── PROMPT_FABLE.md
├── data/
│   ├── events.csv
│   ├── ginf.csv
│   ├── dictionary.txt
│   └── processed_shots/            # Parquet (generat de Cerința 4)
├── models/                         # generate de C4 și C6
│   ├── rf_pipeline_model/          # PipelineModel MLlib (C4)
│   ├── tf_goal_predictor.keras     # rețeaua TF (C6)
│   └── tf_scaler.joblib            # scaler-ul TF (C6)
├── plots/                          # figuri salvate de notebook-uri
├── venv/                           # virtual environment (nu se include în arhivă)
└── README.md
```

> Setul de date e mare → dacă nu intră în arhivă, include un `link_dataset.txt` cu linkul Kaggle.

---

## Jurnalul fix-urilor de rulare (sesiunea de verificare finală)

Probleme descoperite la rularea end-to-end și soluțiile aplicate — utile și ca răspunsuri la întrebări de tip „ce probleme ai întâmpinat?".

### Fix 1 — Directorul `plots/` nu exista la primul `savefig` (C2, C3, C5, C6)
- **Problemă identificată:** `plt.savefig('plots/...')` arunca `FileNotFoundError` pentru că matplotlib nu creează directoare lipsă; `os.makedirs` exista doar într-o celulă ulterioară primului plot.
- **Soluție aplicată:** celula-bootstrap din fiecare notebook creează acum `plots/` și `models/` cu `os.makedirs(..., exist_ok=True)` imediat după fixarea working directory-ului — o singură sursă de adevăr, înaintea oricărui output.
- **De reținut la prezentare:** *De ce în bootstrap și nu lângă fiecare savefig?* Pentru că e idempotent (`exist_ok=True`) și rulează garantat primul — orice celulă ulterioară poate presupune că directoarele există.

### Fix 2 — `decimal.Decimal` din `ROUND()` SQL rupe matplotlib/seaborn (C2 ×4 celule, C5)
- **Problemă identificată:** `ROUND(x, n)` din Spark SQL produce tipul SQL `DECIMAL`, care la `toPandas()` devine `decimal.Decimal` (nu `float`). Simptome diferite, aceeași cauză: `TypeError: Decimal + float` la adnotări (`v + 0.02`), `TypeError: Image data of dtype object` la `sns.heatmap` pe pivot, erori la `min() - 0.05` în limitele axelor. Afectate: C2 §2.5 (rata conversie/zonă), §2.7 (statistici ligi), §2.9 (heatmap situație×corp), §2.10 (xG) și C5 (analiza calității șuturilor).
- **Soluție aplicată:** după fiecare `toPandas()` pe rezultate SQL cu `ROUND`, cast explicit: `df['coloana'] = df['coloana'].astype(float)`.
- **De reținut la prezentare:** *De ce întoarce Spark Decimal?* `ROUND` pe o expresie cu literal zecimal (`*100.0`) dă tipul exact `DECIMAL(p,s)` — corect pentru bani/precizie, dar incompatibil cu aritmetica float din Python. Alternativa în SQL: `CAST(... AS DOUBLE)`. Notă: `round()` din **DataFrame API** pe un `avg()` întoarce `double`, deci nu are problema — doar varianta SQL pe expresii decimale o are.

### Fix-uri anterioare (rezolvate în sesiunile precedente, păstrate aici ca referință)
- **`'NA'` + ANSI mode (Spark 4.x):** cast-ul string-ului `'NA'` la numeric aruncă excepție (nu mai întoarce null ca în Spark 2/3) → `nullValue='NA'` la toate citirile CSV.
- **`isnan()` pe coloane string:** eșuează în Spark 4.x → verificarea null-urilor folosește doar `col(c).isNull()`.
- **Ghilimele drepte în JSON-ul notebook-urilor:** textul românesc cu `„..."` (închidere dreaptă ASCII) rupea string-urile JSON → înlocuite cu ghilimele curbe.

---

## Checklist implementare

- [x] **Cerința 1** — Introducere, link dataset, obiective
- [x] **Cerința 2** — Spark SQL + DataFrame API (+ xG, corelație, evoluție goluri)
- [x] **Cerința 3** — RandomForestClassifier + GBTRegressor (+ weightCol, overfitting check)
- [x] **Cerința 4** — Pipeline ETL → Parquet + Pipeline MLlib serializat/reîncărcat
- [x] **Cerința 5** — UDF + Pandas UDF + benchmark + CrossValidator
- [x] **Cerința 6** — Rețea TensorFlow (+ PR curve, distribuție probabilități, scaler salvat)
- [x] **Cerința 7** — Structured Streaming + inferență ML (3 query-uri, 2 output modes)
- [x] **Rulare completă** a tuturor notebook-urilor cu output-uri salvate (ordine: 2 → 4 → 3 → 5 → 6 → 7) — toate cele 7 notebook-uri rulează fără erori, output-urile sunt salvate în .ipynb, artefactele (Parquet, modele, 21 figuri) generate pe disc
- [x] Verificat că fiecare cod e precedat de descriere în limbaj natural (20 de celule Markdown explicative adăugate în C2–C6: configurare Spark, citire date, filtrare, antrenare/evaluare modele, feature importance, Parquet, serializare pipeline, UDF-uri, CrossValidator, scalare NN)
- [x] Pagină de titlu + cuprins în notebook-ul principal
