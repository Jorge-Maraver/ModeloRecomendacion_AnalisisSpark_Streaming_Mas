#!/usr/bin/env python
"""Bot 'inserso': mayores de 66, jubilados, género aleatorio, ~500 msg/s por defecto."""
import argparse
import json
import random
import time
from confluent_kafka import Producer

def make_payload():
    gender = random.choice(["Hombre", "Mujer"])
    occupation = "Jubilado"
    age = random.randint(66, 90)
    ratings = [{"filmId": i, "rating": random.randint(1, 5)} for i in range(1, 6)]
    return {"gender": gender, "occupation": occupation, "age": age, "ratings": ratings}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default="feedback")
    p.add_argument("--rate", type=float, default=500.0)
    p.add_argument("--count", type=int, default=0)
    args = p.parse_args()

    prod = Producer({"bootstrap.servers": args.bootstrap, "client.id": "bot-inserso"})
    interval = 1.0 / max(args.rate, 1e-6)
    total = 0
    print(f"Bot inserso -> {args.topic} @ ~{args.rate} msg/s (count={args.count or 'inf'})")
    next_ts = time.perf_counter()
    try:
        while args.count == 0 or total < args.count:
            prod.produce(args.topic, json.dumps(make_payload()))
            total += 1
            next_ts += interval
            sleep_for = next_ts - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            if total % 1000 == 0:
                prod.poll(0)
        prod.flush()
    except KeyboardInterrupt:
        pass
    print(f"Enviados {total} mensajes")

if __name__ == "__main__":
    main()
