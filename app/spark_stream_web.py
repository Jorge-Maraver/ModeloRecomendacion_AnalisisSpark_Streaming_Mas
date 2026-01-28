#!/usr/bin/env python
"""Lee JSON de Kafka, acumula los últimos puntos y exporta a app/data/data.json para la web."""
from pathlib import Path
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
import pandas as pd

BOOTSTRAP = "kafka:9092"
TOPIC = "demo"
OUT_PATH = Path("app/data/data.json")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

spark = (SparkSession.builder
         .appName("KafkaSparkWeb")
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

# Acumula puntos en cada batch (tail 200)

def save_batch(df, epoch_id):
    pdf_new = df.orderBy("ts").toPandas()
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text()).get("points", [])
            pdf_old = pd.DataFrame(existing)
        except Exception:
            pdf_old = pd.DataFrame(columns=["ts", "value"])
    else:
        pdf_old = pd.DataFrame(columns=["ts", "value"])
    pdf_all = pd.concat([pdf_old, pdf_new], ignore_index=True)
    pdf_all = pdf_all.drop_duplicates(subset=["ts", "value"])
    pdf_all = pdf_all.sort_values("ts").tail(200)
    data = {"points": pdf_all.to_dict(orient="records")}
    OUT_PATH.write_text(json.dumps(data))
    print(f"Guardado snapshot con {len(pdf_all)} puntos en {OUT_PATH}")

query = (parsed.writeStream
         .outputMode("append")
         .foreachBatch(save_batch)
         .start())

print("Streaming en marcha. Exportando a app/data/data.json")
query.awaitTermination()
