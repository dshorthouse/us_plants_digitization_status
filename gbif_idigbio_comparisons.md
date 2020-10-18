# Apache Spark Queries for Combining GBIF & iDigBio

```bash
$ brew install apache-spark
$ spark-shell --packages org.apache.spark:spark-avro_2.12:3.0.0 --driver-memory 12G
```

```scala
import sys.process._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._

//load the occurrence GBIF DataFrame
val gbif = spark.
    read.
    format("avro").
    load("gbif_plants_avro")

//load the occurrence_raw iDigBio DataFrame
val idigbio = spark.
    read.
    format("avro").
    load("idigbio_plants_combined_avro")

val common = gbif.
    select($"institutionCode", $"collectionCode", $"catalogNumber").
    join(idigbio, $"institutionCode" === $"or_institutionCode" && $"collectionCode" === $"or_collectionCode" && $"catalogNumber" === $"or_catalogNumber", "inner").
    distinct.
    groupBy("institutionCode").
    count.
    withColumnRenamed("count", "commonToGBIFiDigBio").
    repartition(1).
    write.
    mode("overwrite").
    option("header", "true").
    option("quote", "\"").
    option("escape", "\"").
    csv("gbif_idigbio_comparisons/commonToGBIFiDigBio.csv")

val uniquegbif = gbif.
    select($"institutionCode", $"collectionCode", $"catalogNumber").
    join(idigbio, $"institutionCode" === $"or_institutionCode" && $"collectionCode" === $"or_collectionCode" && $"catalogNumber" === $"or_catalogNumber", "left").
    where($"or_catalogNumber".isNull).
    distinct.
    groupBy("institutionCode").
    count.
    withColumnRenamed("count", "uniqueToGBIF").
    repartition(1).
    write.
    mode("overwrite").
    option("header", "true").
    option("quote", "\"").
    option("escape", "\"").
    csv("gbif_idigbio_comparisons/uniqueToGBIF.csv")

val uniqueidigbio = idigbio.
    select($"or_institutionCode", $"or_collectionCode", $"or_catalogNumber").
    join(gbif, $"institutionCode" === $"or_institutionCode" && $"collectionCode" === $"or_collectionCode" && $"catalogNumber" === $"or_catalogNumber", "left").
    where($"catalogNumber".isNull).
    distinct.
    groupBy("or_institutionCode").
    count.
    withColumnRenamed("count", "uniqueToiDigBio").
    repartition(1).
    write.
    mode("overwrite").
    option("header", "true").
    option("quote", "\"").
    option("escape", "\"").
    csv("gbif_idigbio_comparisons/uniqueToiDigBio.csv")


```
