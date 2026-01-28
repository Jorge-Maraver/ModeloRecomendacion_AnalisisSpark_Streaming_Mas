#!/usr/bin/env python
"""Servidor formulario: envía a Kafka y delega la recomendación en app4."""
import json
import logging
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib import request, error

from confluent_kafka import KafkaException, Producer

BOOTSTRAP = "kafka:9092"
TOPIC = "feedback"
ROOT = Path(__file__).parent
RECOMMENDER_URL = "http://localhost:8003/recommend"

producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "form-sender"})

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class Handler(SimpleHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/submit":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
            logging.info("Petición recibida: %s", payload)
            # Enviar a Kafka
            try:
                producer.produce(TOPIC, json.dumps(payload))
                # flush con timeout corto para no bloquear
                producer.flush(1)
            except KafkaException as ke:
                logging.error("Kafka error: %s", ke)
            except Exception as ke:
                logging.error("Error produciendo en Kafka: %s", ke)
            # Delegar recomendación en app4
            req = request.Request(RECOMMENDER_URL, method="POST")
            req.add_header("Content-Type", "application/json")
            data = json.dumps(payload).encode()
            try:
                with request.urlopen(req, data=data, timeout=10) as resp:
                    resp_data = resp.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(resp_data)))
                    self.end_headers()
                    self.wfile.write(resp_data)
            except error.HTTPError as e:
                msg = e.read()
                self.send_response(500)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
            except Exception as e:
                msg = str(e).encode()
                self.send_response(500)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
        except Exception as e:
            msg = str(e).encode()
            self.send_response(400)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def translate_path(self, path):
        # Servir archivos desde app2
        rel = path.lstrip("/") or "index.html"
        return str(ROOT / rel)


if __name__ == "__main__":
    port = 8001
    httpd = HTTPServer(("0.0.0.0", port), Handler)
    print(
        f"Servidor formulario en http://0.0.0.0:{port} (POST -> Kafka '{TOPIC}' + recomendaciones)"
    )
    httpd.serve_forever()
