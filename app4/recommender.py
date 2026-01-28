#!/usr/bin/env python
"""Servicio sencillo que carga el modelo ALS y devuelve top-N recomendaciones para un usuario nuevo.

- Carga el modelo desde /home/jovyan/work/Modelo/Modelo_als/als1
- Carga el catálogo desde datasets/Transformados/movies_mod.parquet
- Endpoint POST /recommend con JSON: { ratings: [{filmId, rating}, ...] }
  Opcionalmente puede incluir gender, occupation, age, pero no se usan en el ALS actual.
- Devuelve top 5 con filmId, title y score.
"""
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pathlib import Path
import json
import numpy as np
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

ROOT = Path(__file__).parent
MODEL_PATH = "/home/jovyan/work/Modelo/Modelo_als/als1"
MOVIES_PATH = "/home/jovyan/work/datasets/Transformados/movies_mod.parquet"
PORT = 8003
TOP_N = 5
REG = 0.1

spark = SparkSession.builder.appName("RecommenderService").getOrCreate()
als_model = ALSModel.load(MODEL_PATH)

# Catálogo
movies_df = spark.read.parquet(MOVIES_PATH).select("filmId", "film")
movies_map = {row.filmId: row.film for row in movies_df.collect()}

# Item factors en driver
item_pdf = als_model.itemFactors.toPandas()
item_ids = item_pdf["id"].to_numpy()
Y = np.vstack(item_pdf["features"].to_numpy())
rank = Y.shape[1]


def recommend_for_ratings(seed_ratings, top_n=TOP_N, reg=REG):
    rated_ids = np.array([r["filmId"] for r in seed_ratings], dtype=int)
    r = np.array([float(r["rating"]) for r in seed_ratings], dtype=float)
    mask = np.isin(item_ids, rated_ids)
    if not mask.any():
        raise ValueError("Ninguna de las pelis está en el modelo")
    Y_r = Y[mask]
    lhs = Y_r.T @ Y_r + reg * np.eye(rank)
    rhs = Y_r.T @ r
    user_vec = np.linalg.solve(lhs, rhs)
    scores = Y @ user_vec
    scores[mask] = -np.inf
    top_idx = np.argpartition(-scores, top_n)[:top_n]
    recs = sorted(zip(item_ids[top_idx], scores[top_idx]), key=lambda x: -x[1])
    return recs


class Handler(SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/recommend":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
            ratings = payload.get("ratings") or []
            if not ratings:
                raise ValueError("Faltan ratings")
            # Guardar la petición en un DataFrame de Spark (for logging/demo)
            schema = StructType([
                StructField("filmId", IntegerType()),
                StructField("rating", DoubleType())
            ])
            rows = [(int(r["filmId"]), float(r["rating"])) for r in ratings]
            spark.createDataFrame(rows, schema=schema).createOrReplaceTempView("last_request_ratings")
            recs = recommend_for_ratings(ratings)
            out = []
            for film_id, score in recs:
                out.append({
                    "filmId": int(film_id),
                    "title": movies_map.get(int(film_id), ""),
                    "score": float(score),
                })
            resp = {"recommendations": out}
            data = json.dumps(resp).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            msg = str(e).encode()
            self.send_response(400)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def translate_path(self, path):
        # Servir archivos desde app4
        rel = path.lstrip("/") or "index.html"
        return str(ROOT / rel)

if __name__ == "__main__":
    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Recommender en http://0.0.0.0:{PORT} (POST /recommend)")
    httpd.serve_forever()
