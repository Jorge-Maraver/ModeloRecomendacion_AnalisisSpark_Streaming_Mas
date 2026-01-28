#!/usr/bin/env python
"""Consulta la tabla en memoria demo_agg y la grafica en tiempo (polling)."""
import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PlotLive").getOrCreate()

plt.ion()
fig, ax = plt.subplots()
line, = ax.plot([], [], "-o")
ax.set_xlabel("ts (ms)")
ax.set_ylabel("value")

while True:
    try:
        pdf = spark.sql("select ts, value from demo_agg order by ts").toPandas()
    except Exception:
        pdf = None
    if pdf is not None and not pdf.empty:
        line.set_data(pdf["ts"], pdf["value"])
        ax.relim(); ax.autoscale_view()
        plt.draw(); plt.pause(0.1)
    else:
        plt.pause(0.5)
    time.sleep(1)
