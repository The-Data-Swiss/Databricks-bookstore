# Databricks notebook source
# MAGIC %sql
# MAGIC USE cloudnail_dev.raw;

# COMMAND ----------

from pyspark.sql import functions as F

SCHEMA = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

query = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .schema(SCHEMA)
                .load("s3://cloudnail-471112974257-dev-raw/bookstore/")
                .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
                .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
            .writeStream
                .option("checkpointLocation", "dbfs:/mnt/raw")
                .option("mergeSchema", True)
                .partitionBy("topic", "year_month")
                .trigger(availableNow=True)
                .table("bookstore")
)

query.awaitTermination()



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from bookstore;

# COMMAND ----------

customer_json_schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_time TIMESTAMP"

query = (spark.readStream
                .table("bookstore")
                .filter(F.col("topic") == "customers")
                .select(F.from_json(F.col("value").cast("string"), customer_json_schema).alias("customer"))
                .select("customer.*")
            .writeStream
                .option("checkpointLocation", "dbfs:/mnt/customers")
                .trigger(availableNow=True)
                .table("customers")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from customers;

# COMMAND ----------

books_json_schema = "book_id STRING, title STRING, author STRING, price STRING, updated STRING"

query = (spark.readStream
                .table("bookstore")
                .filter(F.col("topic") == "books")
                .select(F.from_json(F.col("value").cast("string"), books_json_schema).alias("book"))
                .select("book.*")
                .withColumn("updated", F.from_unixtime(F.col("updated")).cast("timestamp"))
                .dropDuplicates(["book_id", "updated"])
            .writeStream
                .option("checkpointLocation", "dbfs:/mnt/books")
                .trigger(availableNow=True)
                .table("books")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books;

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, \
    IntegerType, StringType, DoubleType, TimestampType

current_book_schema = StructType([
    StructField("book_id", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("price", DoubleType()),
    StructField("updated", TimestampType())
])

if not spark.catalog.tableExists("current_books"):
    spark.catalog.createTable("current_books", schema=current_book_schema)


# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

def upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("books_microbatch")

    current_books_delta_table = DeltaTable.forName(spark, "current_books")

    current_books_delta_table.alias("cb") \
        .merge(microBatchDF.alias("mb"), "mb.book_id = cb.book_id") \
        .whenNotMatchedInsertAll() \
        .whenMatchedUpdateAll() \
        .execute()

# COMMAND ----------

q = (spark
    .readStream
        .table("books")
    .writeStream
        .option("checkpointLocation", "dbfs:/mnt/current_books")
        .foreachBatch(upsert)
        .start()
)

q.awaitTermination()
