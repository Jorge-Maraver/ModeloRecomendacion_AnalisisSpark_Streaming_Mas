#!/usr/bin/env python
"""Servidor que lee Kafka con Spark Streaming y expone counts por HTTP (sin escribir JSON en disco)."""
import json
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

BOOTSTRAP = "kafka:9092"
TOPIC = "feedback"
ROOT = Path(__file__).parent
PORT = 8010

spark = (
    SparkSession.builder.appName("KafkaFeedbackLiveCounts")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

rating_schema = StructType(
    [StructField("filmId", IntegerType()), StructField("rating", IntegerType())]
)
schema = StructType(
    [
        StructField("gender", StringType()),
        StructField("occupation", StringType()),
        StructField("age", IntegerType()),
        StructField("ratings", ArrayType(rating_schema)),
    ]
)

source = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .load()
)

parsed = source.select(
    F.from_json(F.col("value").cast("string"), schema).alias("json")
).select("json.gender", "json.occupation", "json.age")

parsed = parsed.withColumn(
    "age_bin",
    F.when(F.col("age") < 18, "<18")
    .when((F.col("age") >= 18) & (F.col("age") <= 24), "18-24")
    .when((F.col("age") >= 25) & (F.col("age") <= 34), "25-34")
    .when((F.col("age") >= 35) & (F.col("age") <= 44), "35-44")
    .when((F.col("age") >= 45) & (F.col("age") <= 49), "45-49")
    .when((F.col("age") >= 50) & (F.col("age") <= 55), "50-55")
    .otherwise("56+"),
)

# Streaming a tabla en memoria
query = (
    parsed.writeStream.format("memory")
    .queryName("feedback_raw")
    .outputMode("append")
    .start()
)


def compute_counts():
    pdf = spark.sql("select gender, occupation, age_bin from feedback_raw").toPandas()
    if pdf.empty:
        return {"gender": {}, "occupation": {}, "age": {}}
    return {
        "gender": pdf["gender"].value_counts().to_dict(),
        "occupation": pdf["occupation"].value_counts().to_dict(),
        "age": pdf["age_bin"].value_counts().to_dict(),
    }


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/counts"):
            try:
                counts = compute_counts()
                data = json.dumps(counts).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except Exception as e:
                msg = str(e).encode()
                self.send_response(500)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            return
        return super().do_GET()

    def translate_path(self, path):
        # Servir archivos desde app3
        rel = path.lstrip("/") or "index.html"
        return str(ROOT / rel)


def serve_http():
    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"HTTP en http://0.0.0.0:{PORT} (GET /counts) y estáticos de app3/")
    httpd.serve_forever()


if __name__ == "__main__":
    threading.Thread(target=serve_http, daemon=True).start()
    print(
        "Streaming en marcha leyendo de Kafka y almacenando en memoria (feedback_raw)..."
    )
    query.awaitTermination()
