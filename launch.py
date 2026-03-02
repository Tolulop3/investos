"""
InvestOS — Launch
==================
Run once. Keep it open. That's it.

    python launch.py          # full run (~12 min)
    python launch.py --test   # quick test (~3 min)
    python launch.py --no-run # just open dashboard, no analysis

What happens:
  1. Bakes any existing data into dashboard.html immediately
  2. Opens dashboard.html in your browser automatically
  3. Runs today's full analysis in the background
  4. When done → bakes fresh data into dashboard.html
  5. Dashboard auto-reloads with new data
  6. Refresh button → triggers a new run at any time

Keep this terminal window open while using the dashboard.
"""

import http.server
import json
import os
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime

PORT              = 8888
DASHBOARD_FILE    = "dashboard.html"
MARKER_START      = "// INVESTOS_DATA_START"
MARKER_END        = "// INVESTOS_DATA_END"

state = {
    "status":  "idle",
    "step":    "",
    "elapsed": 0,
    "error":   "",
    "started": None,
}


def load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except:
        return None


def bake(brief=None, fx=None, content=None):
    """Inject JSON data directly into dashboard.html between markers."""
    if not os.path.exists(DASHBOARD_FILE):
        print(f"  ⚠️  {DASHBOARD_FILE} not found")
        return False

    brief   = brief   or load_json("latest_brief.json")
    fx      = fx      or load_json("fx_signals.json")
    content = content or load_json("content_output.json")

    if not brief:
        return False

    if brief.get("fx_signals") and not fx:
        fx = brief["fx_signals"]
    if brief.get("content") and not content:
        content = brief["content"]

    baked_json = json.dumps(
        {"brief": brief, "fx": fx or {}, "content": content or {},
         "baked_at": datetime.now().isoformat()},
        default=str, ensure_ascii=False
    )

    with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    if MARKER_START not in html or MARKER_END not in html:
        print("  ⚠️  Markers missing from dashboard.html")
        return False

    s    = html.index(MARKER_START) + len(MARKER_START)
    e    = html.index(MARKER_END)
    html = html[:s] + f"\nconst BAKED = {baked_json};\n" + html[e:]

    with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  ✅ Baked into {DASHBOARD_FILE} ({len(html)//1024} KB)")
    return True


def run_analysis(test_mode=False):
    global state
    state.update({"status": "running", "step": "Starting...",
                  "elapsed": 0, "error": "", "started": datetime.now()})

    STEPS = [
        ("[1/",  "📰 News & macro analysis..."),
        ("[2/",  "📊 Market regime check..."),
        ("[3/",  "🔍 Screening 500+ stocks..."),
        ("[4/",  "🔗 Applying news adjustments..."),
        ("[5/",  "🤖 ML engine (XGBoost)..."),
        ("[6/",  "🧠 RS rankings & intelligence..."),
        ("[7/",  "📡 X signal feeds..."),
        ("[8/",  "🎯 Conviction picks..."),
        ("[9/",  "💱 FX & Gold signals..."),
        ("[10/", "✍️  Social content..."),
    ]

    try:
        cmd  = [sys.executable, "run_daily.py"] + (["--test"] if test_mode else [])
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)

        for line in proc.stdout:
            line = line.rstrip()
            if line:
                print(f"    {line}")
            for kw, label in STEPS:
                if kw in line:
                    state["step"] = label
                    break
            if state["started"]:
                state["elapsed"] = int((datetime.now() - state["started"]).total_seconds())

        proc.wait()

        if proc.returncode == 0:
            state["step"] = "💾 Baking data into dashboard..."
            print("\n  💾 Baking data...")
            bake()
            state["status"] = "done"
            secs = int((datetime.now() - state["started"]).total_seconds())
            print(f"  ✅ Done in {secs//60}m {secs%60}s — dashboard reloading\n")
        else:
            state["status"] = "error"
            state["error"]  = f"run_daily.py failed (exit {proc.returncode})"
            print("  ❌ Run failed\n")

    except FileNotFoundError:
        state["status"] = "error"
        state["error"]  = "run_daily.py not found in this folder"
    except Exception as e:
        state["status"] = "error"
        state["error"]  = str(e)


class Handler(http.server.SimpleHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path.split("?")[0] == "/status":
            elapsed = (int((datetime.now() - state["started"]).total_seconds())
                       if state["started"] else 0)
            self._json({"state": state["status"], "step": state["step"],
                        "elapsed": elapsed, "error": state["error"]})
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/refresh":
            if state["status"] == "running":
                self._json({"ok": False, "msg": "Already running"}, 409)
                return
            state["status"] = "idle"
            threading.Thread(target=run_analysis, daemon=True).start()
            time.sleep(0.2)
            self._json({"ok": True})
        else:
            self._json({"ok": False, "msg": "Not found"}, 404)

    def _json(self, data, code=200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        msg = fmt % args
        if any(x in msg for x in ("404", "500")):
            print(f"  [server] {msg}")


def main():
    test_mode = "--test"   in sys.argv
    no_run    = "--no-run" in sys.argv

    for f in ["run_daily.py", DASHBOARD_FILE]:
        if not os.path.exists(f):
            print(f"\n  ❌ Missing: {f} — all files must be in the same folder\n")
            sys.exit(1)

    print(f"\n{'='*50}")
    print(f"  INVESTOS")
    print(f"  {datetime.now().strftime('%B %d, %Y — %I:%M %p')}")
    print(f"{'='*50}")

    # Bake existing data so dashboard isn't blank on open
    if os.path.exists("latest_brief.json"):
        print(f"\n  📊 Previous data found — baking into dashboard...")
        bake()

    # Start server
    server = http.server.HTTPServer(("localhost", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"\n  🌐 http://localhost:{PORT}/{DASHBOARD_FILE}")

    # Open browser
    time.sleep(0.5)
    webbrowser.open(f"http://localhost:{PORT}/{DASHBOARD_FILE}")
    print(f"  ✅ Browser opened\n")

    # Run analysis
    if no_run:
        print(f"  ⏭  Skipped analysis (--no-run)\n")
    else:
        mode = "TEST MODE (~3 min)" if test_mode else "FULL RUN (~12 min)"
        print(f"  🚀 {mode} — dashboard reloads when done\n")
        run_analysis(test_mode=test_mode)

    print(f"\n{'='*50}")
    print(f"  ✅ Ready. Refresh button triggers new runs.")
    print(f"  Keep this window open. Ctrl+C to quit.")
    print(f"{'='*50}\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Stopped.\n")
        server.shutdown()


if __name__ == "__main__":
    main()
