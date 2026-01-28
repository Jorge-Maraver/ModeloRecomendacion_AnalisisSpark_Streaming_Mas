#!/usr/bin/env python
"""Streaming + conteos en vivo con medición de cada trigger (append, latest)."""
import json
import threading
import time
from datetime import datetime, timezone
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
from pyspark.sql.utils import AnalysisException

BOOTSTRAP = "kafka:9092"
TOPIC = "feedback"
ROOT = Path(__file__).parent
PORT = 8011  # puerto del servidor HTTP para esta variante
METRICS_PATH = ROOT / "metrics_append.csv"

spark = (
    SparkSession.builder.appName("KafkaFeedbackLiveCountsMetrics")
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

METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
if not METRICS_PATH.exists():
    METRICS_PATH.write_text("epoch_id,count,duration_ms,timestamp\n")


def save_and_log(df, epoch_id):
    t0 = time.perf_counter()
    df.select("gender", "occupation", "age_bin").createOrReplaceTempView("feedback_raw")
    count = df.count()
    duration_ms = (time.perf_counter() - t0) * 1000.0
    with METRICS_PATH.open("a") as f:
        f.write(
            f"{epoch_id},{count},{duration_ms:.4f},{datetime.now(tz=timezone.utc).isoformat()}\n"
        )


query = parsed.writeStream.outputMode("append").foreachBatch(save_and_log).start()


def compute_counts():
    try:
        pdf = spark.sql(
            "select gender, occupation, age_bin from feedback_raw"
        ).toPandas()
    except AnalysisException:
        return {"gender": {}, "occupation": {}, "age": {}}
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
            counts = compute_counts()
            data = json.dumps(counts).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        return super().do_GET()

    def translate_path(self, path):
        rel = path.lstrip("/") or "../app3/index.html"
        return str((ROOT.parent / "app3" / Path(rel)).resolve())


def serve_http():
    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"HTTP (append/latest) en http://0.0.0.0:{PORT}")
    httpd.serve_forever()


if __name__ == "__main__":
    threading.Thread(target=serve_http, daemon=True).start()
    print(f"Streaming append/latest, métricas -> {METRICS_PATH}")
    query.awaitTermination()
