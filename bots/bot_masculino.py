#!/usr/bin/env python
"""Bot masculino: hombres 25-40, ocupación aleatoria, ~200 msg/s por defecto."""
import argparse
import json
import random
import time
from confluent_kafka import Producer

OCCUPATIONS = [
    "Otro / No especificado","Académico / Educador","Artista","Administrativo / Oficina",
    "Estudiante universitario / Postgrado","Atención al cliente","Médico / Sector salud",
    "Ejecutivo / Gerente","Agricultor","Amo/a de casa","Estudiante (Escuela/Instituto)",
    "Abogado","Programador","Jubilado","Ventas / Marketing","Científico","Autónomo",
    "Técnico / Ingeniero","Artesano / Oficio manual","Desempleado","Escritor"
]

def make_payload():
    gender = "Hombre"
    occupation = random.choice(OCCUPATIONS)
    age = random.randint(25, 40)
    ratings = [{"filmId": i, "rating": random.randint(1, 5)} for i in range(1, 6)]
    return {"gender": gender, "occupation": occupation, "age": age, "ratings": ratings}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default="feedback")
    p.add_argument("--rate", type=float, default=200.0)
    p.add_argument("--count", type=int, default=0)
    args = p.parse_args()

    prod = Producer({"bootstrap.servers": args.bootstrap, "client.id": "bot-masculino"})
    interval = 1.0 / max(args.rate, 1e-6)
    total = 0
    print(f"Bot masculino -> {args.topic} @ ~{args.rate} msg/s (count={args.count or 'inf'})")
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
