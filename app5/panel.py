#!/usr/bin/env python
"""Panel sencillo para arrancar/parar bots de Kafka.

- Sirve una página en el puerto 8020 (por defecto) con botones start/stop.
- Endpoints:
  GET /status -> JSON con bots en ejecución
  POST /start?bot=nombre -> arranca el bot
  POST /stop?bot=nombre -> detiene el bot

Bots conocidos (ruta relativa):
  random:     bots/bot_random.py
  random50:   bots/bot_random50.py
  revista:    bots/bot_revista.py
  inserso:    bots/bot_inserso.py
  masculino:  bots/bot_masculino.py

Ajusta BOT_SCRIPTS si cambias nombres/rutas.
"""
import json
import subprocess
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).parent
PORT = 8020

BOT_SCRIPTS = {
    "random": ROOT.parent / "bots" / "bot_random.py",
    "random50": ROOT.parent / "bots" / "bot_random50.py",
    "revista": ROOT.parent / "bots" / "bot_revista.py",
    "inserso": ROOT.parent / "bots" / "bot_inserso.py",
    "masculino": ROOT.parent / "bots" / "bot_masculino.py",
}

# Gestion de procesos
processes = {}


def start_bot(name):
    path = BOT_SCRIPTS.get(name)
    if not path or not path.exists():
        raise ValueError(f"Bot desconocido o no existe: {name}")
    if name in processes and processes[name].poll() is None:
        return "ya_en_ejecucion"
    proc = subprocess.Popen(["python", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    processes[name] = proc
    return "ok"


def stop_bot(name):
    proc = processes.get(name)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        return "ok"
    return "no_encontrado"


def status():
    out = {}
    for name, proc in processes.items():
        out[name] = proc.poll() is None
    return out


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/status":
            data = json.dumps(status()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        bot = qs.get("bot", [None])[0]
        if parsed.path == "/start":
            try:
                result = start_bot(bot)
                data = json.dumps({"status": result}).encode()
                self.send_response(200)
            except Exception as e:
                data = str(e).encode()
                self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if parsed.path == "/stop":
            result = stop_bot(bot)
            data = json.dumps({"status": result}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_error(404)

    def translate_path(self, path):
        rel = path.lstrip('/') or 'index.html'
        return str(ROOT / rel)


def serve():
    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Panel bots en http://0.0.0.0:{PORT} (start/stop/status)")
    httpd.serve_forever()


if __name__ == "__main__":
    serve()
