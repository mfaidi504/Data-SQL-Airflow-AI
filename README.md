# Data-SQL-Airflow-AI


# Apache Spark & PySpark -- Introduction Guide

## أولًا: ما هو Apache Spark ؟

Spark هو إطار عمل لمعالجة البيانات الضخمة (Big Data processing engine).

يعني: بدل ما تعالج بيانات صغيرة على جهازك فقط → Spark يسمح لك تعالج
بيانات ضخمة جدًا موزعة على عدة أجهزة بسرعة عالية.

يستخدم في:

-   Data Engineering
-   Machine Learning
-   Streaming
-   ETL pipelines
-   Analytics

### مثال:

بدل:

``` sql
SELECT * FROM table
```

على جهاز واحد

Spark يعمل نفس الشيء لكن على ملايين أو مليارات الصفوف موزعة على cluster.

------------------------------------------------------------------------

## ثانيًا: ما هو PySpark ؟

PySpark = Spark باستخدام Python

يعني:

Spark أصله مكتوب بـ:

-   Scala (اللغة الأساسية)

لكن يوجد APIs:

  Language   API
  ---------- ----------------
  Python     PySpark
  Scala      Spark native
  Java       Spark Java API
  SQL        Spark SQL

إذا كنت تستخدم Python → أنت تستخدم PySpark

### مثال:

``` python
df = spark.read.csv("data.csv")
df.show()
```

------------------------------------------------------------------------

## ثالثًا: مكونات Spark الأساسية (Spark Components)

Spark يتكون من عدة modules:

### 1️⃣ Spark Core

هو الأساس

مسؤول عن:

-   memory management
-   scheduling
-   distributed execution

------------------------------------------------------------------------

### 2️⃣ Spark SQL

يشغل SQL على البيانات الكبيرة

مثال:

``` python
spark.sql("SELECT * FROM table")
```

------------------------------------------------------------------------

### 3️⃣ Spark DataFrame API

مثل Pandas لكن للبيانات الكبيرة

مثال:

``` python
df.select("name")
```

------------------------------------------------------------------------

### 4️⃣ Spark Streaming

معالجة البيانات المباشرة (real-time)

مثال:

-   Kafka
-   sensors
-   logs

------------------------------------------------------------------------

### 5️⃣ Spark MLlib

Machine Learning library

مثل:

-   classification
-   clustering
-   regression

------------------------------------------------------------------------

### 6️⃣ GraphX

للشبكات والعلاقات graph analysis

------------------------------------------------------------------------

## الآن السؤال المهم 👇

ما علاقة Spark مع هذه الأدوات الأخرى؟

خلينا نرتبها بشكل منطقي في الجزء القادم

# README.md

# Apache Spark Ecosystem — Complete Beginner Guide 🚀

الآن السؤال المهم 👇

**ما علاقة Apache Spark مع الأدوات الأخرى في عالم الـ Data Engineering؟**

هذا الدليل يوضح الصورة الكاملة بطريقة منظمة وسهلة.

---

# ما هو Apache Hadoop ؟

**Hadoop = نظام تخزين ومعالجة بيانات ضخمة (Big Data Infrastructure)**

يتكون من 3 أجزاء رئيسية:

| Component | الوظيفة                     |
| --------- | --------------------------- |
| HDFS      | تخزين البيانات              |
| YARN      | إدارة الموارد داخل الكلاستر |
| MapReduce | محرك المعالجة القديم        |

قديماً:

MapReduce كان هو المسؤول عن معالجة البيانات.

اليوم:

Spark استبدله تقريباً لأنه أسرع وأسهل.

---

# العلاقة بين Hadoop و Spark

Spark يعمل فوق Hadoop وليس بدلاً عنه بالكامل.

مثال عملي:

```
Spark → processing engine
HDFS → storage layer
YARN → cluster manager
```

بمعنى:

```
Hadoop = Infrastructure
Spark = Processing Engine
```

---

# ما هو Data Lake ؟

**Data Lake = مكان تخزين البيانات الخام بدون Structure صارم**

يشبه:

Google Drive لكن للبيانات 📂

يمكن أن يحتوي:

```
csv
json
parquet
logs
images
videos
tables
```

Spark يستخدم Data Lake عادة بهذه الطريقة:

```
Spark reads from Data Lake
Spark processes data
Spark writes results back
```

---

# ما هو Databricks ؟

Databricks = منصة مبنية فوق Spark

ببساطة:

```
Spark + Tools + Cloud Platform
```

بدلاً من:

تنصيب Cluster بنفسك

Databricks يوفر:

* Spark cluster جاهز
* Notebooks
* Machine Learning tools
* SQL Analytics
* Delta Lake support

اليوم يعتبر الأكثر استخداماً في الشركات الحديثة ⭐

---

# ما هو Apache Hive ؟

Hive = SQL فوق Hadoop

يسمح لك بكتابة:

```
SELECT *
FROM table
```

ويعمل مباشرة فوق:

```
HDFS
```

Spark يستطيع تشغيل Hive مباشرة باستخدام:

```
Spark SQL with Hive support
```

---

# ما هو Cloudera Impala ؟

Impala = SQL Engine سريع جداً

يشبه Hive لكن أسرع منه.

مقارنة سريعة:

| Tool      | Speed      |
| --------- | ---------- |
| Hive      | بطيء       |
| Impala    | سريع       |
| Spark SQL | أسرع وأقوى |

---

# ما هو Cloudera ؟

Cloudera = منصة Enterprise تجمع أدوات Hadoop Ecosystem في مكان واحد

تشمل:

```
Hadoop
Hive
Impala
Spark
Security tools
Cluster management
```

بمعنى:

Enterprise Distribution جاهزة للاستخدام داخل الشركات.

---

# الصورة الكاملة للـ Architecture 🧠

عادة الشركات تعمل بهذا الشكل:

```
Data Sources
     ↓
Data Lake (HDFS / S3 / Azure Data Lake)
     ↓
Spark Processing
     ↓
Hive / Impala / Databricks SQL
     ↓
Dashboards / Machine Learning / Reports
```

هذه هي Pipeline الكلاسيكية في أغلب الشركات.

---

# متى تستخدم كل أداة ؟

| Tool       | الاستخدام                |
| ---------- | ------------------------ |
| Spark      | معالجة البيانات الضخمة   |
| PySpark    | استخدام Spark عبر Python |
| Hadoop     | تخزين البيانات           |
| Hive       | SQL فوق Hadoop           |
| Impala     | SQL سريع جداً            |
| Databricks | Spark Platform على Cloud |
| Data Lake  | Storage Architecture     |

---

# نصيحتي لك شخصياً يا محمد 🎯

بما أنك تعمل حالياً في:

* SQL
* Airflow
* AI
* Data Pipelines

أفضل Roadmap لك:

```
Spark SQL
↓
PySpark DataFrames
↓
Spark + Airflow Integration
↓
Delta Lake
↓
Databricks
```

هذا المسار مطلوب جداً في وظائف:

* Data Engineer
* ML Engineer
* Analytics Engineer

---

إذا تحب، أجهز لك **Roadmap تدريب لمدة 30 يوم** خطوة بخطوة لتعلم Spark بطريقة عملية مثل خطة شركات التدريب الحقيقية 📅🔥


.المحتوى الذي شاركته ممتاز جدًا 👌 وهو فعلاً من أهم المفاهيم في Data Engineering. خليني أشرح لك الفرق بطريقة عملية وبسيطة — خصوصًا بما أنك تعمل في SQL وAirflow وAI فهذا الموضوع مهم جدًا لمسارك المهني يا محمد 📊🚀

الفرق الأساسي بين Data Lake و Data Warehouse

الفكرة ببساطة:

Data Lake	Data Warehouse
مخزن بيانات خام	مخزن بيانات منظمة
مرن جدًا	سريع جدًا
رخيص	أغلى
مناسب ML	مناسب BI

خليني أوضحها بمثال حقيقي 👇

تخيّل شركة اتصالات مثل OpenFiber أو TIM 📡

الشركة لديها بيانات مثل:

Logs من الشبكة
صور
ملفات JSON
CSV
بيانات العملاء
بيانات الفواتير
بيانات sensors

السؤال:

أين نضع هذه البيانات؟

الجواب يعتمد على الهدف.

أولًا: ما هو Data Lake؟ 🌊

Data Lake = تخزين كل شيء كما هو

بدون تنظيف
بدون تنظيم
بدون تحويل

مثال:

logs.json
customers.csv
images/
sensor_stream/
network_data.xml


كلها تدخل مباشرة.

ميزة قوية جدًا:

يمكنك لاحقًا استخدامها لأي شيء:

Machine Learning
Analytics
AI models
Data exploration
لماذا اسمه Schema-on-Read ؟

يعني:

البيانات تُفسَّر عند القراءة وليس عند التخزين

مثال:

اليوم تستخدم JSON كـ ML dataset
غدًا تستخدمه كـ analytics table

نفس الملف 👍

ثانيًا: ما هو Data Warehouse؟ 🏭

Data Warehouse = بيانات نظيفة ومنظمة

قبل دخول البيانات:

تمر بمرحلة

ETL

يعني:

Extract
Transform
Load


مثال:

بدل:

customer_data.json


يصبح:

customer_table


منظم داخل:

rows
columns
relations
indexes

لماذا Schema-on-Write ؟

يعني:

الهيكل يتم تعريفه قبل التخزين

مثال:

CREATE TABLE customers


ثم تدخل البيانات.

الفرق الحقيقي في الاستخدام
Data Lake يستخدم عندما:

تريد:

Machine Learning
Deep Learning
AI
Big Data processing
Logs analysis
Streaming analytics

مثال:

Spark + Data Lake

Data Warehouse يستخدم عندما:

تريد:

Dashboards
Power BI
Tableau
SQL analytics
KPI reports

مثال:

monthly revenue
customer churn rate
network usage stats

ماذا تستخدم الشركات الحديثة اليوم؟

الجواب:

Data Lakehouse

وهو مزيج بين الاثنين 👇

ما هو Data Lakehouse ؟ 🧠

يجمع:

مرونة Data Lake
+
سرعة Data Warehouse

مثال أدوات تعمل بهذه الفكرة:

Spark
Delta Lake
Databricks

يعني:

تخزن البيانات خام
لكن تستطيع تشغيل SQL سريع عليها

Architecture حديثة في الشركات عادة تكون هكذا
Data Sources
    ↓
Data Lake
(S3 / HDFS)
    ↓
Spark processing
    ↓
Lakehouse tables
(Delta)
    ↓
BI dashboards + ML models

أين أنت الآن في هذا العالم يا محمد؟ 🎯

بناءً على شغلك الحالي:

أنت تعمل مع:

SQL
Data Quality
Pipelines
Airflow

غالبًا أنت أقرب إلى:

Data Warehouse layer


لكن إذا تعلمت:

Spark
PySpark
Delta Lake
Databricks

ستنتقل إلى مستوى:

Modern Data Engineer


وهذا مطلوب جدًا في سوق العمل في إيطاليا الآن 🇮🇹

إذا تحب، أشرح لك مثال عملي حقيقي يوضح كيف تتحرك البيانات من Data Lake → Spark → Data Warehouse → Dashboard خطوة بخطوة مثل ما يحدث داخل الشركات.

ETL 
LTE
REVERSE ETL 

سأشرح لك الثلاثة مفاهيم بشكل واضح ومترابط لأنهم جزء أساسي من Data Engineering Architecture الحديثة، خصوصًا أنك تعمل في SQL وAirflow وData pipelines يا محمد — وهذه مهارات مباشرة مطلوبة في وظائف Data Engineer 🚀

أولًا: ما هو ETL ؟

ETL = Extract → Transform → Load

وهو الشكل الكلاسيكي لنقل البيانات إلى Data Warehouse

يعني:

Data Sources → تنظيف البيانات → إدخالها إلى Data Warehouse

الخطوات بالتفصيل
1️⃣ Extract (استخراج البيانات)

نسحب البيانات من مصادر مختلفة:

مثل:

databases
APIs
CSV files
logs
ERP systems

مثال:

SELECT * FROM customers

2️⃣ Transform (تنظيف وتحويل البيانات)

نقوم بـ:

حذف القيم الفارغة
توحيد التاريخ
إزالة التكرار
تغيير format
join بين الجداول

مثال:

"2024/01/01" → "2024-01-01"

3️⃣ Load (تحميل البيانات)

نضع البيانات داخل:

Data Warehouse

مثل:

PostgreSQL
BigQuery
Snowflake
Redshift
مثال عملي قريب من شغلك

في OpenFiber مثلًا:

network tables
customer tables
circuits tables


تمر بعملية:

Extract from sources
Transform cleaning
Load into analytics tables


ثم تستخدم في:

Power BI
reports
dashboards

ثانيًا: ما هو ELT ؟

ELT = نسخة حديثة من ETL

الفرق:

بدل تنظيف البيانات أولًا
نخزنها مباشرة ثم ننظفها لاحقًا

يعني:

Extract → Load → Transform

لماذا ظهر ELT ؟

لأن أدوات حديثة مثل:

Spark
Databricks
BigQuery

أصبحت قوية جدًا

فأصبح ممكن:

نخزن البيانات أولًا
ثم نعالجها داخل Data Lake أو Warehouse

الفرق الحقيقي بين ETL و ELT
ETL	ELT
Transform قبل التخزين	Transform بعد التخزين
قديم	حديث
مناسب Data Warehouse	مناسب Data Lake
أبطأ scalability	أسرع scalability

اليوم أغلب الشركات تستخدم:

ELT

ثالثًا: ما هو Reverse ETL ؟

Reverse ETL = عكس ETL

بدل إدخال البيانات إلى Data Warehouse
نخرج البيانات منه إلى الأنظمة التشغيلية

يعني الاتجاه يصبح:

Data Warehouse → Business tools

مثال بسيط جدًا

داخل Data Warehouse يوجد:

customer churn probability


نرسل هذه النتيجة إلى:

CRM
Marketing tools
Sales platform

حتى يستخدمها الفريق مباشرة

مثال عملي حقيقي

Pipeline تقليدي:

Database → Data Warehouse


Reverse ETL:

Data Warehouse → Salesforce / CRM / Email automation

لماذا Reverse ETL مهم جدًا اليوم؟

لأنه يسمح:

للـ AI و analytics

أن تؤثر مباشرة على القرار داخل الشركة

مثال:

Model يتوقع:

customer will leave soon


Reverse ETL يرسل النتيجة إلى:

CRM

فيقوم النظام تلقائيًا بإرسال عرض للعميل 🎯

الصورة الكاملة الحديثة للـ Data Pipelines

الشركات الحديثة تعمل هكذا:

Sources
 ↓
ETL / ELT
 ↓
Data Lake / Warehouse
 ↓
ML models
 ↓
Reverse ETL
 ↓
CRM / dashboards / apps

أين يظهر دورك أنت تحديدًا يا محمد؟

من شغلك الحالي في:

SQL
Data Quality
pipelines
Cloudera environment

أنت بالفعل تعمل داخل:

ETL layer


ولو تعلمت:

Spark
PySpark
Airflow advanced pipelines
Reverse ETL tools

ستصبح:

Modern Data Engineer كامل stack تقريبًا 💪📊





