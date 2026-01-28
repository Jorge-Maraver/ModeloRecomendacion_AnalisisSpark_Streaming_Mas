#!/usr/bin/env python
"""Productor sencillo que envía JSON aleatorios a Kafka."""
import json
import random
import time
from confluent_kafka import Producer

BOOTSTRAP = "kafka:9092"
TOPIC = "demo"

producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "bot"})

print(f"Enviando JSON a topic '{TOPIC}' en {BOOTSTRAP} (CTRL+C para salir)")
try:
    while True:
        payload = {"ts": int(time.time() * 1000), "value": round(random.uniform(0, 10), 3)}
        producer.produce(TOPIC, json.dumps(payload))
        producer.flush()
        print("->", payload)
        time.sleep(1)
except KeyboardInterrupt:
    pass
