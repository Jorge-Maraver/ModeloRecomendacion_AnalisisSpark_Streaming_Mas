#!/usr/bin/env python
"""Bot que envía payloads aleatorios al topic Kafka `feedback`.

- Genera género, ocupación y edad aleatorios (opcionalmente ratings dummy).
- Permite ajustar tasa (--rate msg/s) y número total (--count). Con --count 0 envía infinito.
"""
import argparse
import json
import random
import time
from confluent_kafka import Producer

GENDERS = ["Hombre", "Mujer"]
OCCUPATIONS = [
    "Otro / No especificado","Académico / Educador","Artista","Administrativo / Oficina",
    "Estudiante universitario / Postgrado","Atención al cliente","Médico / Sector salud",
    "Ejecutivo / Gerente","Agricultor","Amo/a de casa","Estudiante (Escuela/Instituto)",
    "Abogado","Programador","Jubilado","Ventas / Marketing","Científico","Autónomo",
    "Técnico / Ingeniero","Artesano / Oficio manual","Desempleado","Escritor"
]


def make_payload():
    gender = random.choice(GENDERS)
    occupation = random.choice(OCCUPATIONS)
    age = random.randint(18, 70)
    ratings = [{"filmId": i, "rating": random.randint(1, 5)} for i in range(1, 6)]
    return {"gender": gender, "occupation": occupation, "age": age, "ratings": ratings}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092", help="Bootstrap servers")
    parser.add_argument("--topic", default="feedback", help="Kafka topic")
    parser.add_argument("--rate", type=float, default=100.0, help="Mensajes por segundo (aprox)")
    parser.add_argument("--count", type=int, default=0, help="Total de mensajes (0 = infinito)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "bot-random"})
    interval = 1.0 / max(args.rate, 1e-6)
    total = 0
    print(f"Enviando a {args.topic} en {args.bootstrap} a ~{args.rate} msg/s (count={args.count or 'infinito'})")
    next_ts = time.perf_counter()
    try:
        while args.count == 0 or total < args.count:
            payload = make_payload()
            producer.produce(args.topic, json.dumps(payload))
            total += 1
            # throttling simple
            next_ts += interval
            sleep_for = next_ts - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            if total % 1000 == 0:
                producer.poll(0)
        producer.flush()
    except KeyboardInterrupt:
        pass
    print(f"Enviados {total} mensajes")


if __name__ == "__main__":
    main()
