// Databricks notebook source
// MAGIC %md
// MAGIC ![](https://s3.us-east-2.amazonaws.com/databricks-roy/wos.jpeg)

// COMMAND ----------

// MAGIC %md
// MAGIC # MOCK EVENTS

// COMMAND ----------

import java.sql.Date
import scala.util.Random

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

def offset(dd: String = "01"): Date = Date.valueOf(s"2018-01-$dd")
val random = Random

case class Event(
  source: String,
  date: Date, 
  value: Int
)

val events = (1 to 20)
  .toList
  .map(n => Event(if(n%2==0) "A" else "B", offset(s"0${n%7+1}"), random.nextInt(128)))
  .toDS
  .orderBy($"source", $"date")

display(events)

// COMMAND ----------

events
  .write
  .format("parquet")
  .mode("overwrite")
  .saveAsTable("roy.windows")

// COMMAND ----------

// MAGIC %md
// MAGIC # Windows in `Scala`

// COMMAND ----------

val n = 5

val windowSpec = Window.partitionBy("source").orderBy(asc("date")).rowsBetween((1-n), 0)

display(
  spark
    .table("roy.windows")
    .withColumn("window_values", collect_list($"value").over(windowSpec))
    .filter(size($"window_values") === n)
    .withColumn("window_avg", avg($"value").over(windowSpec))
    .withColumn("window_stddev", stddev($"value").over(windowSpec))
    .withColumn("z_score", abs($"value" - $"window_avg")/$"window_stddev")
    .orderBy(asc("source"), asc("date"))
)

// COMMAND ----------

display(
  spark
    .table("roy.windows")
    .groupBy($"source", window($"date", s"$n days"))
    .agg(
      collect_list($"value").alias("window_values"), 
      avg($"value").alias("window_avg"), 
      stddev($"value").alias("window_stddev"))
    .orderBy(asc("source"), asc("window.start"))
)

// COMMAND ----------

// MAGIC %md
// MAGIC # Windows in `Python`

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.functions import asc, col, collect_list, size, avg, stddev, abs, asc, window
// MAGIC from pyspark.sql.window import Window

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC n = 5
// MAGIC 
// MAGIC window_spec = Window.partitionBy("source").orderBy(asc("date")).rowsBetween((1-n), 0)
// MAGIC 
// MAGIC display(
// MAGIC   spark
// MAGIC     .table("roy.windows")
// MAGIC     .withColumn("window_values", collect_list(col("value")).over(window_spec))
// MAGIC     .filter(size(col("window_values")) == n)
// MAGIC     .withColumn("window_avg", avg(col("value")).over(window_spec))
// MAGIC     .withColumn("window_stddev", stddev(col("value")).over(window_spec))
// MAGIC     .withColumn("z_score", abs(col("value") - col("window_avg") )/col("window_stddev") )
// MAGIC     .orderBy(asc("source"), asc("date"))
// MAGIC )

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(
// MAGIC   spark
// MAGIC     .table("roy.windows")
// MAGIC     .groupBy(col("source"), window(col("date"), "%d days" % n))
// MAGIC     .agg(
// MAGIC       collect_list(col("value") ).alias("window_values"), 
// MAGIC       avg(col("value")).alias("window_avg"), 
// MAGIC       stddev(col("value")).alias("window_stddev"))
// MAGIC     .orderBy(asc("source"), asc("window.start"))
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC # Windows in `R`

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC library("SparkR")
// MAGIC library("magrittr")

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC n <- 5
// MAGIC 
// MAGIC windowSpec <- rowsBetween(orderBy(windowPartitionBy("source"), "date"), 1-n, 0)
// MAGIC 
// MAGIC display(
// MAGIC   tableToDF("roy.windows") %>%
// MAGIC     withColumn("window_values", over(collect_list(column("value")), windowSpec) ) %>%
// MAGIC     filter(size(column("window_values")) == n) %>%
// MAGIC     withColumn("window_avg", over(avg(column("value")), windowSpec) ) %>%
// MAGIC     withColumn("window_stddev", over(stddev(column("value")), windowSpec) ) %>%
// MAGIC     withColumn("z_score", abs(column("value") - column("window_avg") )/column("window_stddev") ) %>%
// MAGIC     orderBy(asc(column("source")), asc(column("date")))
// MAGIC )

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC display(
// MAGIC   tableToDF("roy.windows") %>%
// MAGIC     groupBy(column("source"), window(column("date"), sprintf("%d days",n))) %>%
// MAGIC     agg(
// MAGIC       alias(collect_list(column("value") ), "window_values"), 
// MAGIC       alias(avg(column("value")), "window_avg"), 
// MAGIC       alias(stddev(column("value")), "window_stddev")) %>%
// MAGIC     orderBy(asc(column("source")), asc(column("window.start")))
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC # Windows in `SQL`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT 
// MAGIC   *, 
// MAGIC   abs(value-window_avg)/window_stddev as z_score 
// MAGIC FROM (
// MAGIC   SELECT 
// MAGIC     source, 
// MAGIC     date,
// MAGIC     value,
// MAGIC     collect_list(value) OVER (PARTITION BY source ORDER BY date ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS window_values,
// MAGIC     avg(value) OVER (PARTITION BY source ORDER BY date ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS window_avg,
// MAGIC     stddev(value) OVER (PARTITION BY source ORDER BY date ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS window_stddev
// MAGIC   FROM 
// MAGIC     roy.windows)
// MAGIC WHERE
// MAGIC   size(window_values) == 4
// MAGIC ORDER BY
// MAGIC   source ASC, date ASC

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT 
// MAGIC   source, 
// MAGIC   window(date, '5 days') as window,
// MAGIC   collect_list(value) AS window_values,
// MAGIC   avg(value) AS window_avg,
// MAGIC   stddev(value) AS window_stddev
// MAGIC FROM 
// MAGIC   roy.windows
// MAGIC GROUP BY
// MAGIC   source,
// MAGIC   window
// MAGIC ORDER BY
// MAGIC   window.start ASC, source ASC

// COMMAND ----------

