from collections import deque
from threading import Lock

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Telecom Metrics API")

HISTORY_LIMIT = 20


class MetricsWindow(BaseModel):
    window_start: str
    window_end: str
    avg_cpu: float
    avg_latency: float
    total_events: int
    error_count: int
    error_rate: float
    unique_servers: int


_latest = None
_history = deque(maxlen=HISTORY_LIMIT)
_lock = Lock()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/metrics")
def post_metrics(window: MetricsWindow):
    global _latest

    with _lock:
        _latest = window
        _history.append(window)

    return {"status": "received"}


@app.get("/metrics")
def get_latest_metrics():
    with _lock:
        if _latest is None:
            return {"status": "waiting", "message": "No metrics received yet"}

        return _latest


@app.get("/metrics/history")
def get_history():
    with _lock:
        return list(_history)


@app.get("/", response_class=HTMLResponse)
def dashboard_page():
    return """
<!DOCTYPE html>
<html>
<head>
  <title>Telecom Live Metrics</title>
  <style>
    body { background:#0d1117; color:#c9d1d9; font-family: monospace; padding: 2rem; }
    h1 { color:#58a6ff; }
    table { border-collapse: collapse; margin-top: 1rem; }
    td { padding: 0.4rem 1.2rem 0.4rem 0; font-size: 1.1rem; }
    td.label { color:#8b949e; }
    #status { color:#8b949e; font-size:0.9rem; }
  </style>
</head>
<body>
  <h1>Telecom Live Metrics</h1>
  <div id="status">Connecting...</div>
  <table id="metrics"></table>
  <script>
    async function refresh() {
      try {
        const res = await fetch('/metrics');
        const data = await res.json();
        const status = document.getElementById('status');
        const table = document.getElementById('metrics');

        if (data.status === 'waiting') {
          status.textContent = data.message;
          table.innerHTML = '';
          return;
        }

        status.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        table.innerHTML = `
          <tr><td class="label">Window</td><td>${data.window_start} -> ${data.window_end}</td></tr>
          <tr><td class="label">Average CPU</td><td>${data.avg_cpu.toFixed(2)}%</td></tr>
          <tr><td class="label">Average Latency</td><td>${data.avg_latency.toFixed(2)} ms</td></tr>
          <tr><td class="label">Total Events</td><td>${data.total_events}</td></tr>
          <tr><td class="label">Errors</td><td>${data.error_count}</td></tr>
          <tr><td class="label">Error Rate</td><td>${data.error_rate.toFixed(2)}%</td></tr>
          <tr><td class="label">Unique Servers</td><td>${data.unique_servers}</td></tr>
        `;
      } catch (e) {
        document.getElementById('status').textContent = 'Waiting for API...';
      }
    }

    refresh();
    setInterval(refresh, 1000);
  </script>
</body>
</html>
"""
