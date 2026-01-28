#!/usr/bin/env python
"""Spark Structured Streaming: lee JSON de Kafka y mantiene tabla en memoria."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

BOOTSTRAP = "kafka:9092"
TOPIC = "demo"

spark = (SparkSession.builder
         .appName("KafkaSparkMatplotlib")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .getOrCreate())

schema = StructType([
    StructField("ts", LongType()),
    StructField("value", DoubleType()),
])

source = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("subscribe", TOPIC)
          .load())

parsed = (source
          .select(F.from_json(F.col("value").cast("string"), schema).alias("json"))
          .select("json.*"))

# Mantén los últimos 200 registros en memoria
stream = (parsed.orderBy(F.desc("ts")).limit(200)
          .writeStream
          .format("memory")
          .queryName("demo_agg")
          .outputMode("complete")
          .start())

print("Streaming en marcha. Consulta con spark.sql('select * from demo_agg').show()")
stream.awaitTermination()
