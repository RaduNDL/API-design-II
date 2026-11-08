import csv
import hashlib
import hmac
import http.client
import io
import json
import re
import secrets
import sqlite3
import time
import uuid

from contextlib import contextmanager
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parent
WEB = ROOT / "Web"
DB_PATH = ROOT / "catalog.sqlite3"
HOST = "127.0.0.1"
API_PORT = 8000
WEBHOOK_PORT = 8001
MAX_BYTES = 256 * 1024
MAX_ROWS = 1000
FINAL_STATES = {"succeeded", "completed_with_errors", "failed"}
SECRET = secrets.token_bytes(32)
STOP = Event()

STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/produse.csv": ("produse.csv", "text/csv; charset=utf-8"),
}


@contextmanager
def database():
    connection = sqlite3.connect(DB_PATH, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize():
    with database() as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS imports (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                csv_data TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                processed INTEGER NOT NULL DEFAULT 0,
                total INTEGER,
                saved INTEGER NOT NULL DEFAULT 0,
                errors TEXT NOT NULL DEFAULT '[]',
                hook_status TEXT,
                hook_attempts INTEGER NOT NULL DEFAULT 0,
                hook_next REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS products (
                sku TEXT PRIMARY KEY,
                price_cents INTEGER NOT NULL CHECK (price_cents >= 0)
            );

            CREATE TABLE IF NOT EXISTS webhook_inbox (
                event_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            );
        """)


def csv_reader(source):
    return csv.reader(io.StringIO(source, newline=""), strict=True)


def get_import(import_id):
    with database() as db:
        row = db.execute(
            """SELECT id, mode, status, processed, total, saved, errors,
                      hook_status, hook_attempts
               FROM imports WHERE id = ?""",
            (import_id,),
        ).fetchone()

    if row is None:
        return None

    return {
        "id": row["id"],
        "mode": row["mode"],
        "status": row["status"],
        "processedRows": row["processed"],
        "totalRows": row["total"],
        "savedRows": row["saved"],
        "errors": json.loads(row["errors"]),
        "webhook": {
            "status": row["hook_status"],
            "attempts": row["hook_attempts"],
        },
    }


def finish_import(job_id, status, products, errors):

    with database() as db:
        db.executemany(
            """INSERT INTO products (sku, price_cents) VALUES (?, ?)
               ON CONFLICT(sku) DO UPDATE SET price_cents = excluded.price_cents""",
            products,
        )
        db.execute(
            """UPDATE imports
               SET status = ?, saved = ?, errors = ?, csv_data = '',
                   hook_status = 'pending', hook_next = 0
               WHERE id = ?""",
            (status, len(products), json.dumps(errors, ensure_ascii=False), job_id),
        )


def process_import(job):
    job_id = job["id"]
    reader = csv_reader(job["csv_data"])

    try:
        next(reader)
        rows = [(number, row) for number, row in enumerate(reader, 2) if row]
    except csv.Error:
        finish_import(job_id, "failed", [], [
            {"message": "Fișierul conține o linie CSV formatată incorect."}
        ])
        return

    if not 1 <= len(rows) <= MAX_ROWS:
        finish_import(job_id, "failed", [], [
            {"message": f"Fișierul trebuie să aibă între 1 și {MAX_ROWS} produse."}
        ])
        return

    with database() as db:
        db.execute("UPDATE imports SET total = ? WHERE id = ?", (len(rows), job_id))

    valid = []
    errors = []
    seen = set()

    for processed, (number, row) in enumerate(rows, 1):

        if len(rows) <= 10:
            time.sleep(0.7)

        sku, price = (value.strip() for value in row) if len(row) == 2 else ("", "")
        if len(row) != 2 or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", sku):
            errors.append({"row": number, "message": "Coloane sau cod de produs invalid."})
        elif sku in seen:
            errors.append({"row": number, "sku": sku, "message": "Cod repetat în fișier."})
        elif not re.fullmatch(r"[0-9]{1,8}(?:\.[0-9]{1,2})?", price):
            errors.append({
                "row": number,
                "sku": sku,
                "message": "Prețul trebuie să fie pozitiv sau zero, cu cel mult două zecimale.",
            })
        else:
            seen.add(sku)
            valid.append((sku, int(Decimal(price) * 100)))

        with database() as db:
            db.execute("UPDATE imports SET processed = ? WHERE id = ?", (processed, job_id))

    if errors and (job["mode"] == "atomic" or not valid):
        finish_import(job_id, "failed", [], errors)
    else:
        status = "completed_with_errors" if errors else "succeeded"
        finish_import(job_id, status, valid, errors)


def import_worker():
    while not STOP.is_set():
        try:
            with database() as db:
                job = db.execute(
                    """SELECT id, mode, csv_data FROM imports
                       WHERE status IN ('queued', 'running') ORDER BY rowid LIMIT 1"""
                ).fetchone()
                if job:
                    db.execute(
                        "UPDATE imports SET status = 'running', processed = 0 WHERE id = ?",
                        (job["id"],),
                    )

            if job:
                process_import(job)
            else:
                STOP.wait(0.15)
        except sqlite3.Error as error:
            print(f"Eroare SQLite la import: {error}", flush=True)
            STOP.wait(1)


def signature(body):
    return hmac.new(SECRET, body, hashlib.sha256).hexdigest().encode("ascii")


def deliver_webhook(job):
    payload = json.dumps({
        "id": f"import.finished:{job['id']}",
        "type": "import.finished",
        "importId": job["id"],
        "status": job["status"],
    }).encode("utf-8")

    connection = http.client.HTTPConnection(HOST, WEBHOOK_PORT, timeout=3)
    try:
        connection.request("POST", "/webhooks/imports", payload, {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature(payload).decode("ascii"),
        })
        with connection.getresponse() as response:
            response.read()
            return 200 <= response.status < 300
    except (OSError, http.client.HTTPException):
        return False
    finally:
        connection.close()


def webhook_worker():
    while not STOP.is_set():
        try:
            with database() as db:
                job = db.execute(
                    """SELECT id, status, hook_attempts FROM imports
                       WHERE hook_status = 'pending' AND hook_next <= ?
                       ORDER BY rowid LIMIT 1""",
                    (time.time(),),
                ).fetchone()

            if job is None:
                STOP.wait(0.2)
                continue

            delivered = deliver_webhook(job)
            attempts = job["hook_attempts"] + 1
            status = "delivered" if delivered else ("failed" if attempts >= 3 else "pending")
            with database() as db:
                db.execute(
                    """UPDATE imports SET hook_status = ?, hook_attempts = ?, hook_next = ?
                       WHERE id = ?""",
                    (status, attempts, time.time() + attempts, job["id"]),
                )
        except sqlite3.Error as error:
            print(f"Eroare SQLite la webhook: {error}", flush=True)
            STOP.wait(1)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body, content_type, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.close_connection = True
        self.wfile.write(body)

    def json(self, status, data, headers=None):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.reply(status, body, "application/json; charset=utf-8", headers)

    def read_body(self, limit):
        size = int(self.headers.get("Content-Length", "0"))
        if size <= 0:
            raise ValueError("Cererea nu conține date.")
        if size > limit:
            raise OverflowError("Fișierul este prea mare.")
        body = self.rfile.read(size)
        if len(body) != size:
            raise ValueError("Cererea a fost întreruptă.")
        return body

    def log_message(self, format_string, *args):

        pass


class Api(Handler):
    def do_POST(self):
        url = urlsplit(self.path)
        if url.path != "/imports":
            return self.json(404, {"error": "Adresă necunoscută."})

        mode = parse_qs(url.query).get("mode", ["partial"])[0]
        if mode not in {"partial", "atomic"}:
            return self.json(400, {"error": "Modul trebuie să fie partial sau atomic."})
        if self.headers.get_content_type() != "text/csv":
            return self.json(415, {"error": "Trimite fișierul cu tipul text/csv."})

        try:
            source = self.read_body(MAX_BYTES).decode("utf-8-sig")
            header = next(csv_reader(source), None)
        except OverflowError as error:
            return self.json(413, {"error": str(error)})
        except UnicodeDecodeError:
            return self.json(400, {"error": "Salvează fișierul CSV."})
        except (ValueError, csv.Error) as error:
            return self.json(400, {"error": str(error)})

        if header != ["sku", "price"]:
            return self.json(400, {"error": "Prima linie trebuie să fie sku,price."})

        job_id = uuid.uuid4().hex
        with database() as db:
            db.execute(
                "INSERT INTO imports (id, mode, csv_data) VALUES (?, ?, ?)",
                (job_id, mode, source),
            )

        location = f"/imports/{job_id}"
        self.json(202, {
            "id": job_id,
            "status": "queued",
            "statusUrl": location,
        }, {"Location": location})

    def do_GET(self):
        route = urlsplit(self.path).path
        if route in STATIC_FILES:
            name, mime = STATIC_FILES[route]
            return self.reply(200, (WEB / name).read_bytes(), mime)

        if route == "/catalog":
            with database() as db:
                products = db.execute("SELECT * FROM products ORDER BY sku").fetchall()
            return self.json(200, [
                {
                    "sku": row["sku"],
                    "price": f"{row['price_cents'] // 100}.{row['price_cents'] % 100:02d}",
                }
                for row in products
            ])

        match = re.fullmatch(r"/imports/([a-f0-9]{32})(/events)?", route)
        job = get_import(match[1]) if match else None
        if job is None:
            return self.json(404, {"error": "Importul nu a fost găsit."})

        if match[2]:
            self.stream(job)
        else:
            self.json(200, job)

    def stream(self, job):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

        last = None
        heartbeat = time.monotonic()
        try:
            while job is not None and not STOP.is_set():
                if job != last:
                    payload = json.dumps(job, ensure_ascii=False)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    last = job
                if job["status"] in FINAL_STATES:
                    return
                if time.monotonic() - heartbeat >= 10:
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                    heartbeat = time.monotonic()
                STOP.wait(0.2)
                job = get_import(job["id"])
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass


class WebhookReceiver(Handler):
    def do_GET(self):
        if urlsplit(self.path).path != "/webhooks/imports":
            return self.json(404, {"error": "Adresă necunoscută."})

        with database() as db:
            received_events = db.execute("SELECT COUNT(*) FROM webhook_inbox").fetchone()[0]

        self.json(200, {
            "status": "ok",
            "message": "Receptorul webhook este activ și primește notificări prin POST.",
            "receivedEvents": received_events,
            "applicationUrl": f"http://{HOST}:{API_PORT}/",
        })

    def do_POST(self):
        if self.path != "/webhooks/imports":
            return self.json(404, {"error": "Adresă necunoscută."})

        try:
            body = self.read_body(4096)
        except (ValueError, OverflowError):
            return self.json(400, {"error": "Mesaj invalid."})

        received_signature = self.headers.get("X-Webhook-Signature", "").encode("utf-8")
        if not hmac.compare_digest(received_signature, signature(body)):
            return self.json(403, {"error": "Semnătură invalidă."})

        event = json.loads(body)
        with database() as db:
            stored = db.execute(
                "INSERT OR IGNORE INTO webhook_inbox (event_id, payload) VALUES (?, ?)",
                (event["id"], body.decode("utf-8")),
            ).rowcount

        if stored:
            print(f"Webhook primit: {event['type']} pentru {event['importId']}", flush=True)
        self.json(200, {"received": True, "duplicate": not bool(stored)})


def main():
    with ThreadingHTTPServer((HOST, API_PORT), Api) as api:
        with ThreadingHTTPServer((HOST, WEBHOOK_PORT), WebhookReceiver) as receiver:
            initialize()
            Thread(target=receiver.serve_forever, daemon=True).start()
            Thread(target=import_worker, daemon=True).start()
            Thread(target=webhook_worker, daemon=True).start()

            print(f"Aplicația: http://{HOST}:{API_PORT}/", flush=True)
            print(f"Stare receptor webhook: http://{HOST}:{WEBHOOK_PORT}/webhooks/imports", flush=True)
            try:
                api.serve_forever()
            except KeyboardInterrupt:
                print("\nServer oprit.", flush=True)
            finally:
                STOP.set()
                receiver.shutdown()


if __name__ == "__main__":
    main()