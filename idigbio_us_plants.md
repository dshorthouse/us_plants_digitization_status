# Apache Spark Set-Up and Queries

Downloaded dataset from GBIF: [https://doi.org/10.15468/dl.sbuavy](https://doi.org/10.15468/dl.sbuavy)

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

// List of terms we care about
val terms = List(
  "coreid",
  "idigbio:uuid",
  "dwc:institutionCode",
  "dwc:collectionCode",
  "dwc:catalogNumber",
  "dwc:occurrenceID",
  "dwc:kingdom",
  "dwc:scientificName",
  "idigbio:recordset",
  "dwc:locality",
  "idigbio:isoCountryCode",
  "dwc:recordedBy",
  "dwc:eventDate",
  "idigbio:geoPoint",
  "idigbio:hasImage"
)

//TODO: have to get identifiedBy and dateIdentified in occurrence_raw.csv
var alt_terms = List(
  "coreid",
  "dwc:identifiedBy",
  "dwc:dateIdentified",
  "dwc:acceptedNameUsage"
)

var kingdoms = List(
  "plantae",
  "chromista",
  "fungi"
)

//load a big, tsv file from a DwC-A download
val df = spark.
    read.
    format("csv").
    option("header", "true").
    option("mode", "DROPMALFORMED").
    option("delimiter", ",").
    option("quote", "\"").
    option("escape", "\"").
    option("treatEmptyValuesAsNulls", "true").
    option("ignoreLeadingWhiteSpace", "true").
    load("/Users/shorthoused/Downloads/iDigBio/occurrence.csv").
    select(terms.map(col):_*).
    filter(lower($"dwc:kingdom").isin(kingdoms:_*)).
    withColumnRenamed("idigbio:uuid", "uuid").
    withColumnRenamed("dwc:institutionCode", "institutionCode").
    withColumnRenamed("dwc:collectionCode", "collectionCode").
    withColumnRenamed("dwc:catalogNumber", "catalogNumber").
    withColumnRenamed("dwc:occurrenceID", "occurrenceID").
    withColumnRenamed("dwc:kingdom", "kingdom").
    withColumnRenamed("dwc:scientificName", "scientificName").
    withColumnRenamed("dwc:locality", "locality").
    withColumnRenamed("idigbio:recordset", "datasetKey").
    withColumnRenamed("idigbio:isoCountryCode", "isoCountryCode").
    withColumnRenamed("dwc:recordedBy", "recordedBy").
    withColumnRenamed("dwc:eventDate", "eventDate").
    withColumnRenamed("idigbio:geoPoint", "hasCoordinate").
    withColumnRenamed("idigbio:hasImage", "hasImage").
    withColumnRenamed("coreid", "df1_coreid").
    withColumn("hasImage", when($"hasImage".contains("true"), 1).otherwise(lit(null))).
    withColumn("hasCoordinate", when($"hasCoordinate".isNotNull, 1).otherwise(lit(null)))

//optionally save the DataFrame to disk so we don't have to do the above again
df.write.mode("overwrite").format("avro").save("idigbio_plants_occurrence")

//load the saved DataFrame, can later skip the above processes and start from here
val df = spark.
    read.
    format("avro").
    load("idigbio_plants_occurrence")

//Other dataframe from other csv file
val df2 = spark.
  read.
  format("csv").
  option("header", "true").
  option("mode", "DROPMALFORMED").
  option("delimiter", ",").
  option("quote", "\"").
  option("escape", "\"").
  option("treatEmptyValuesAsNulls", "true").
  option("ignoreLeadingWhiteSpace", "true").
  load("/Users/shorthoused/Downloads/iDigBio/occurrence_raw.csv").
  select(alt_terms.map(col):_*).
  withColumnRenamed("dwc:identifiedBy", "identifiedBy").
  withColumnRenamed("dwc:dateIdentified", "dateIdentified").
  withColumnRenamed("dwc:acceptedNameUsage", "acceptedNameUsage")
  withColumnRenamed("coreid", "df2_coreid")

df2.write.mode("overwrite").format("avro").save("idigbio_plants_occurrence_raw")

//load the saved DataFrame, can later skip the above processes and start from here
val df2 = spark.
    read.
    format("avro").
    load("idigbio_plants_occurrence_raw")

val idigbio = df.
    join(df2, $"df1_coreid" === $"df2_coreid", "leftouter").
    groupBy("institutionCode").
    agg(
      count("df1_coreid").alias("total"),
      count("collectionCode").alias("has_collectionCode"),
      count("catalogNumber").alias("has_catalogNumber"),
      count("scientificName").alias("has_scientificName"),
      count("acceptedNameUsage").alias("has_acceptedNameUsage"),
      count("locality").alias("has_locality"),
      count("isoCountryCode").alias("has_countryCode"),
      count("hasCoordinate").alias("has_coordinates"),
      count("hasImage").alias("has_image"),
      count("dateIdentified").alias("has_dateIdentified"),
      count("identifiedBy").alias("has_identifiedBy"),
      count("recordedBy").alias("has_recordedBy"),
      count("eventDate").alias("has_eventDate")
    ).
    repartition(1).
    write.
    mode("overwrite").
    option("header", "true").
    option("quote", "\"").
    option("escape", "\"").
    csv("idigbio_institutionCode_summmary")

//Get the list of iDigBio recordsets so we can whittle them down to those that are US-based
val lists = spark.
    read.
    format("csv").
    option("header", "true").
    option("mode", "DROPMALFORMED").
    option("delimiter", ",").
    option("quote", "\"").
    option("escape", "\"").
    option("treatEmptyValuesAsNulls", "true").
    option("ignoreLeadingWhiteSpace", "true").
    load("/Users/shorthoused/Scripts/us_plants_digitization_status/idigbio_us_datasets.csv").
    withColumnRenamed("uuid", "lists_uuid")

df.join(lists, $"datasetKey" === $"lists_uuid", "inner").
  select("datasetKey", "name").
  distinct.
  repartition(1).
  write.
  mode("overwrite").
  option("header", "true").
  option("quote", "\"").
  option("escape", "\"").
  csv("idigbio_recordsets_list")
