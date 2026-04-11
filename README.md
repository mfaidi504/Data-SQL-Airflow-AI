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

خلينا نرتبها بشكل منطقي في الجزء القادم.


