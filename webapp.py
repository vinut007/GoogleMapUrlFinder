"""
Google Maps URL scraper - web frontend (Flask).

Run:  python webapp.py
Then open: http://127.0.0.1:5000
"""
import argparse
import os
import sys
import threading
import time
from collections import deque

from flask import Flask, abort, jsonify, render_template, request, send_file

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pythonurl as scraper

BASE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(BASE, "work")
os.makedirs(WORK, exist_ok=True)
OUT_CSV = os.path.join(WORK, "out.csv")
UPLOAD_CSV = os.path.join(WORK, "upload.csv")

app = Flask(__name__)
logs = deque(maxlen=2000)
job = {"running": False, "started": 0.0, "src": ""}


class Tee:
    def __init__(self, stream):
        self.stream = stream

    def write(self, s):
        if s.strip():
            logs.append(s.rstrip())
        self.stream.write(s)
        self.stream.flush()

    def flush(self):
        pass


def run_job(src, out, workers, limit, start):
    args = argparse.Namespace(
        src=src, out=out, workers=workers, limit=limit,
        start=start, count=0, headed=False,
    )
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout = Tee(old_out)
        sys.stderr = Tee(old_err)
        scraper.stop_requested = False
        scraper.run_scrape(args)
        logs.append("=== JOB FINISHED ===")
    except Exception as e:
        logs.append(f"=== JOB CRASHED: {e} ===")
    finally:
        sys.stdout, sys.stderr = old_out, old_err
        job["running"] = False


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    st = scraper.stats
    elapsed = time.time() - job["started"] if job["running"] else 0
    saved = len(scraper.done_ids)
    return jsonify({
        "running": job["running"],
        "src": job["src"],
        "total": scraper.CURRENT_TOTAL,
        "saved": saved,
        "ok": st["ok"],
        "fail": st["fail"],
        "matched": st["matched"],
        "elapsed": round(elapsed, 1),
        "rate": round(saved / elapsed * 60, 1) if elapsed > 0 else 0,
        "has_output": os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0,
    })


@app.route("/api/logs")
def api_logs():
    after = request.args.get("after", 0, type=int)
    items = list(logs)
    return jsonify({"after": len(items), "lines": items[after:]})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        abort(400)
    f.save(UPLOAD_CSV)
    return jsonify({"ok": True, "src": UPLOAD_CSV, "name": f.filename})


@app.route("/api/start", methods=["POST"])
def api_start():
    if job["running"]:
        return jsonify({"ok": False, "error": "Job already running"}), 409
    body = request.get_json(silent=True) or {}
    src = (body.get("src") or "").strip() or (
        UPLOAD_CSV if os.path.exists(UPLOAD_CSV) else ""
    )
    if not src or not os.path.exists(src):
        return jsonify({"ok": False, "error": f"Source CSV not found: {src}"}), 400
    workers = max(1, min(int(body.get("workers") or 50), 100))
    limit = int(body.get("limit") or 0)
    start = int(body.get("start") or 0)
    job["running"] = True
    job["started"] = time.time()
    job["src"] = src
    t = threading.Thread(target=run_job, args=(src, OUT_CSV, workers, limit, start), daemon=True)
    t.start()
    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    if job["running"]:
        scraper.stop_requested = True
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "No job running"}), 400


@app.route("/download")
def download():
    if not os.path.exists(OUT_CSV) or os.path.getsize(OUT_CSV) == 0:
        abort(404)
    return send_file(OUT_CSV, as_attachment=True, download_name="508K_Rel_MM_Accounts_with_urls.csv")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
