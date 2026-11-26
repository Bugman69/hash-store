"""End-to-end test: fake AxeOS miner + the real Hub HTTP server. Run: python3 tests/test_hub.py"""
import json, os, sys, tempfile, threading, time, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

tmp = tempfile.mkdtemp()
os.environ.update(HUB_DATA=tmp, HUB_ALLOW_LOOPBACK="1", HUB_POLL_SECONDS="5", HUB_PORT="0")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402

MINER = {"stratumURL": "umbrel.local", "stratumPort": 2018, "stratumUser": "x", "restarts": 0}


class FakeAxe(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _j(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        self._j({"hashRate": 9800.0, "temp": 55, "vrTemp": 60, "power": 101, "sharesAccepted": 40,
                 "sharesRejected": 1, "ASICModel": "BM1373", "hostname": "nexus", "stratumURL": MINER["stratumURL"],
                 "stratumPort": MINER["stratumPort"], "uptimeSeconds": 99})

    def do_PATCH(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        MINER.update(stratumURL=body["stratumURL"], stratumPort=body["stratumPort"], stratumUser=body["stratumUser"], last_patch=body)
        self._j({})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0)); MINER["restarts"] += 1; self._j({})


HOOKS = []  # alerts received by the fake webhook receiver


class FakeHook(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_POST(self):
        HOOKS.append({"title": self.headers.get("Title"), "body": self.rfile.read(int(self.headers["Content-Length"])).decode()})
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()


hook = HTTPServer(("127.0.0.1", 0), FakeHook)
threading.Thread(target=hook.serve_forever, daemon=True).start()
SECRET = "s3cr3ttopic"
HOOK_URL = f"http://127.0.0.1:{hook.server_address[1]}/{SECRET}"


def wait_hooks(n, t=3):
    end = time.time() + t
    while len(HOOKS) < n and time.time() < end: time.sleep(0.05)
    return len(HOOKS) >= n


fake = HTTPServer(("127.0.0.1", 0), FakeAxe)
threading.Thread(target=fake.serve_forever, daemon=True).start()
fake_host = f"127.0.0.1:{fake.server_address[1]}"

hub = app.ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
threading.Thread(target=hub.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{hub.server_address[1]}"


def call(method, path, body=None, headers=True):
    h = {"Content-Type": "application/json"}
    if headers: h["X-HashHub"] = "1"
    req = urllib.request.Request(base + path, method=method, headers=h,
                                 data=None if method == "GET" else json.dumps(body or {}).encode())
    try:
        with urllib.request.urlopen(req) as r: return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond: sys.exit(1)


BTC = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
BCH = "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a"

check("dashboard served", urllib.request.urlopen(base + "/").read().startswith(b"<!doctype html>"))
check("POST without header is rejected", call("POST", "/api/config", {}, headers=False)[0] == 403)
check("bad BTC address rejected", call("POST", "/api/config", {"addresses": {"bitcoin": "notanaddress"}})[0] == 400)
check("config saved", call("POST", "/api/config", {"stratum_host": "192.168.1.50", "addresses": {"bitcoin": BTC, "bitcoincash": BCH}})[0] == 200)
check("public host refused as miner", call("POST", "/api/miners", {"name": "evil", "host": "8.8.8.8"})[0] == 400)
check("miner added", call("POST", "/api/miners", {"name": "Nexus S1", "host": fake_host})[0] == 200)

app.poll_miner(app.CFG["miners"][0])
s = call("GET", "/api/state")[1]
m = s["miners"][0]
check("telemetry normalised (GH/s -> TH/s)", m["online"] and abs(m["hashrate_ths"] - 9.8) < 1e-6)
check("current pool recognised as Bitcoin", m["coin"] == "bitcoin")
check("totals computed", s["totals"]["hashrate_ths"] == 9.8 and s["totals"]["power_w"] == 101)

st, _ = call("POST", "/api/miners/Nexus%20S1/coin", {"coin": "bitcoincash"})
check("switch to BCH succeeds", st == 200)
check("miner got BCH stratum settings", MINER["stratumPort"] == 3334 and MINER["stratumURL"] == "192.168.1.50"
      and MINER["stratumUser"] == BCH + ".Nexus-S1" or MINER["stratumUser"] == BCH + ".NexusS1")
check("miner was restarted", MINER["restarts"] == 1)
app.poll_miner(app.CFG["miners"][0])
check("dashboard now shows BCH", call("GET", "/api/state")[1]["miners"][0]["coin"] == "bitcoincash")

app.COINS["scrypt-test"] = {"id": "scrypt-test", "name": "T", "ticker": "T", "algo": "scrypt", "stratum_port": 9999}
check("algo mismatch refused", call("POST", "/api/miners/Nexus%20S1/coin", {"coin": "scrypt-test"})[0] == 400)

check("schedule added", call("POST", "/api/schedules", {"miners": "*", "coin": "bitcoincash", "start": "00:00", "end": "23:59", "default_coin": "bitcoin"})[0] == 200)
check("bad schedule time rejected", call("POST", "/api/schedules", {"coin": "bitcoin", "start": "25:00", "end": "01:00"})[0] == 400)
check("overnight window logic", app.in_window("22:00", "06:00", 23 * 60) and app.in_window("22:00", "06:00", 3 * 60)
      and not app.in_window("22:00", "06:00", 12 * 60))
want = app.desired_coins()
check("scheduler wants BCH now", want.get("Nexus S1") == "bitcoincash")

# ---- fallback pools
check("fallback with bad port rejected", call("POST", "/api/config", {"fallbacks": {"bitcoincash": {"host": "pool.example.org", "port": "99999"}}})[0] == 400)
check("fallback with path in host rejected", call("POST", "/api/config", {"fallbacks": {"bitcoin": {"host": "pool.example.org/x", "port": "3333"}}})[0] == 400)
check("BCH fallback saved", call("POST", "/api/config", {"fallbacks": {"bitcoincash": {"host": "stratum+tcp://bch.pool.example.org", "port": "3333", "user": ""}}})[0] == 200)
call("POST", "/api/miners/Nexus%20S1/coin", {"coin": "bitcoincash"})
p = MINER["last_patch"]
check("fallback written to miner (scheme stripped, address reused, worker appended)",
      p["fallbackStratumURL"] == "bch.pool.example.org" and p["fallbackStratumPort"] == 3333
      and p["fallbackStratumUser"].startswith(BCH + "."))
call("POST", "/api/miners/Nexus%20S1/coin", {"coin": "bitcoin"})
check("coin with no fallback clears the old one", MINER["last_patch"]["fallbackStratumURL"] == "" and MINER["last_patch"]["fallbackStratumUser"] == "")
app.poll_miner(app.CFG["miners"][0])
call("POST", "/api/miners/Nexus%20S1/coin", {"coin": "bitcoincash"})
app.poll_miner(app.CFG["miners"][0])
before = MINER["restarts"]
check("push to miners re-applies settings", call("POST", "/api/apply")[1].get("updated") == 1 and MINER["restarts"] == before + 1)

# ---- alerts
check("bad webhook scheme rejected", call("POST", "/api/config", {"alerts": {"webhook": "ftp://x/y"}})[0] == 400)
check("metadata-range webhook rejected", call("POST", "/api/config", {"alerts": {"webhook": "http://169.254.169.254/x"}})[0] == 400)
check("alert delay below 30s rejected", call("POST", "/api/config", {"alerts": {"offline_after_s": 5}})[0] == 400)
check("webhook saved", call("POST", "/api/config", {"alerts": {"webhook": HOOK_URL, "format": "ntfy", "offline_after_s": 60}})[0] == 200)
check("webhook secret never sent to browser", SECRET not in json.dumps(call("GET", "/api/state")[1]))
check("test alert delivered", call("POST", "/api/alerts/test")[0] == 200 and wait_hooks(1) and HOOKS[-1]["title"] == "HashHub test alert")

app.CFG["alerts"]["offline_after_s"] = 0          # skip the waiting for the test
app.LAST_SWITCH.clear(); n = len(HOOKS)
app.STATUS["Nexus S1"] = {"online": False, "error": "timed out", "ts": time.time()}
app.check_alerts(); app.check_alerts()
check("offline alert fires once", wait_hooks(n + 1) and "offline" in HOOKS[-1]["title"].lower() and len(HOOKS) == n + 1)
check("dashboard lists active alert", {"kind": "offline", "name": "Nexus S1"} in call("GET", "/api/state")[1]["alerts"]["active"])
app.STATUS["Nexus S1"] = {"online": True, "hashrate_ths": 9.8, "using_fallback": False, "ts": time.time()}
app.check_alerts()
check("recovery alert fires", wait_hooks(n + 2) and "back online" in HOOKS[-1]["title"].lower())
app.STATUS["Nexus S1"] = {"online": True, "hashrate_ths": 0.0, "using_fallback": False, "ts": time.time()}
app.check_alerts()
check("online-but-0-TH/s alert fires", wait_hooks(n + 3) and "not hashing" in HOOKS[-1]["title"].lower())
app.STATUS["Nexus S1"] = {"online": True, "hashrate_ths": 9.8, "using_fallback": True, "ts": time.time()}
app.check_alerts()
check("fallback-active alert fires", wait_hooks(n + 5) and any("fallback" in h["title"].lower() for h in HOOKS[n:]))
app.STATUS["Nexus S1"] = {"online": True, "hashrate_ths": 9.8, "using_fallback": False, "ts": time.time()}
app.check_alerts(); app.CFG["alerts"]["offline_after_s"] = 60
time.sleep(0.5)  # let queued webhook threads from the recovery transitions land
n = len(HOOKS); app.LAST_SWITCH["Nexus S1"] = time.time()
app.STATUS["Nexus S1"] = {"online": False, "error": "rebooting", "ts": time.time()}
app.CFG["alerts"]["offline_after_s"] = 0; app.check_alerts(); time.sleep(0.3)
check("no alert while a switched miner reboots", len(HOOKS) == n)
app.CFG["alerts"]["offline_after_s"] = 60
check("webhook can be removed", call("POST", "/api/config", {"alerts": {"clear_webhook": True}})[0] == 200 and app.CFG["alerts"]["webhook"] == "")

check("config persisted to disk", json.loads((Path(tmp) / "config.json").read_text())["addresses"]["bitcoin"] == BTC)
check("remove miner", call("DELETE", "/api/miners/Nexus%20S1")[0] == 200 and call("GET", "/api/state")[1]["miners"] == [])
print("all passed")
