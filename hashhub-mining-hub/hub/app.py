#!/usr/bin/env python3
"""HashHub: multi-coin mining control plane for Umbrel. Standard library only.

Polls AxeOS-style miners (Bitaxe, Nexus S1, Harlo-OS best effort), serves a
dashboard, and repoints miners between solo stratum servers on demand or on a
schedule. It does not mine and does not run a stratum server.
"""
import base64
import ipaddress
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import parse_qs, unquote, urlparse
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DATA = Path(os.environ.get("HUB_DATA", "/data"))
PORT = int(os.environ.get("HUB_PORT", "8420"))
POLL_SECONDS = max(5, int(os.environ.get("HUB_POLL_SECONDS", "10")))
ALLOW_LOOPBACK = os.environ.get("HUB_ALLOW_LOOPBACK") == "1"  # tests only
RETRY_SECONDS = int(os.environ.get("HUB_RETRY_SECONDS", "300"))
MINER_TYPES = ("axeos", "harlo")

if os.environ.get("TZ") and hasattr(time, "tzset"):
    time.tzset()

LOCK = threading.RLock()
STATUS = {}       # miner name -> latest telemetry
NODES = {}        # coin id -> node status
LAST_SWITCH = {}  # miner name -> time of last switch attempt
EVENTS = []       # recent events for the dashboard
ALERT_STATE = {}  # (kind, name) -> {"since": ts|None, "fired": bool}
SWITCH_GRACE = 90  # seconds after a coin switch during which a rebooting miner raises no alerts
ALERT_FORMATS = ("ntfy", "discord", "slack", "json")
HISTORY_DAYS = max(1, int(os.environ.get("HUB_HISTORY_DAYS", "30")))
SAMPLE_SECONDS = max(10, int(os.environ.get("HUB_SAMPLE_SECONDS", "60")))
DB_LOCK = threading.Lock()
LAST_REC = {}      # miner name -> time of last stored sample
LAST_PRUNE = [0]
# range -> (span seconds, bucket seconds). Charts get 120-360 points whatever the range.
RANGES = {"1h": (3600, 60), "24h": (86400, 300), "7d": (7 * 86400, 1800), "30d": (30 * 86400, 7200)}
METRICS = {"hashrate": "hashrate_ths", "power": "power_w", "temp": "temp"}  # whitelist: names go into SQL


class HubError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def log(msg):
    with LOCK:
        EVENTS.append({"t": time.strftime("%H:%M:%S"), "msg": msg})
        del EVENTS[:-100]
    print(msg, flush=True)


# ---------------------------------------------------------------- config

def load_cfg():
    try:
        cfg = json.loads((DATA / "config.json").read_text())
    except (OSError, ValueError):
        cfg = {}
    cfg.setdefault("stratum_host", os.environ.get("HUB_STRATUM_HOST", "umbrel.local"))
    cfg.setdefault("addresses", {})
    cfg.setdefault("miners", [])
    cfg.setdefault("schedules", [])
    cfg.setdefault("fallbacks", {})  # coin id -> {"host", "port", "user"}
    cfg.setdefault("alerts", {})
    cfg["alerts"].setdefault("webhook", "")
    cfg["alerts"].setdefault("format", "ntfy")
    cfg["alerts"].setdefault("offline_after_s", 120)
    return cfg


def save_cfg():
    DATA.mkdir(parents=True, exist_ok=True)
    tmp = DATA / "config.json.tmp"
    tmp.write_text(json.dumps(CFG, indent=2))
    tmp.replace(DATA / "config.json")


def load_coins():
    coins = {}
    for d in (APP_DIR / "coins", DATA / "coins"):  # data dir overrides built-ins
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.json")):
            try:
                c = json.loads(f.read_text())
                for k in ("id", "name", "ticker", "algo", "stratum_port"):
                    if k not in c:
                        raise ValueError(f"missing {k}")
                if not re.fullmatch(r"[a-z0-9_-]{1,24}", c["id"]):
                    raise ValueError("bad id")
                c["stratum_port"] = int(c["stratum_port"])
                coins[c["id"]] = c
            except (OSError, ValueError, TypeError) as e:
                print(f"skipping coin manifest {f}: {e}", flush=True)
    return coins


CFG = load_cfg()
COINS = load_coins()


def next_slot(used):
    """Chart color slot 1-8 (the validated categorical palette); 0 means 'no slot left'."""
    return next((i for i in range(1, 9) if i not in used), 0)


def ensure_slots():
    """A miner or coin keeps its chart color for life, so filtering or removing others never repaints it."""
    used = {m["slot"] for m in CFG["miners"] if m.get("slot")}
    for m in CFG["miners"]:
        if "slot" not in m:
            m["slot"] = next_slot(used)
            used.add(m["slot"])
    cs = CFG.setdefault("coin_slots", {})
    used = {v for v in cs.values() if v}
    for cid in COINS:
        if cid not in cs:
            cs[cid] = next_slot(used)
            used.add(cs[cid])


ensure_slots()

# ---------------------------------------------------------------- helpers

def http(method, url, body=None, headers=None, timeout=4):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read(1_000_000)
    stripped = raw.lstrip()[:1]
    return json.loads(raw) if stripped in (b"{", b"[") else raw.decode(errors="replace")


def valid_host(h):
    """Miners must be on the LAN: private/link-local IP, .local name or bare hostname."""
    host, _, port = h.partition(":")
    if not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", host) or (port and not port.isdigit()):
        return False
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback:
            return ALLOW_LOOPBACK
        return ip.is_private or ip.is_link_local
    except ValueError:
        return host.endswith(".local") or "." not in host


def worker_name(name):
    return re.sub(r"[^A-Za-z0-9_-]", "", name)[:32] or "miner"


def coin_for_port(port):
    try:
        port = int(port)
    except (TypeError, ValueError):
        return None
    for c in COINS.values():
        if c["stratum_port"] == port:
            return c["id"]
    return None


def find_miner(name):
    for m in CFG["miners"]:
        if m["name"] == name:
            return m
    raise HubError("unknown miner", 404)


# ---------------------------------------------------------------- polling

def poll_miner(m):
    try:
        info = http("GET", f"http://{m['host']}/api/system/info")
        if not isinstance(info, dict):
            raise ValueError("response was not JSON (is this an AxeOS device?)")
        st = {
            "online": True,
            "hashrate_ths": round(float(info.get("hashRate") or 0) / 1000, 3),  # AxeOS reports GH/s
            "temp": info.get("temp"), "vr_temp": info.get("vrTemp"),
            "power_w": info.get("power"),
            "accepted": info.get("sharesAccepted"), "rejected": info.get("sharesRejected"),
            "best_diff": info.get("bestDiff"), "uptime_s": info.get("uptimeSeconds"),
            "model": info.get("ASICModel") or info.get("deviceModel"),
            "pool": f"{info.get('stratumURL')}:{info.get('stratumPort')}",
            "coin": coin_for_port(info.get("stratumPort")),
            "using_fallback": bool(info.get("isUsingFallbackStratum")),
            "ts": time.time(),
        }
    except Exception as e:  # network errors, bad JSON, timeouts
        st = {"online": False, "error": str(e)[:120], "ts": time.time()}
    with LOCK:
        STATUS[m["name"]] = st


def node_status(coin):
    env = coin.get("node_env")
    if not env:
        return {"state": "not_configured"}
    host, port = os.environ.get(env.get("host", "")), os.environ.get(env.get("port", ""))
    user, pw = os.environ.get(env.get("user", ""), ""), os.environ.get(env.get("pass", ""), "")
    if not host or not port:
        return {"state": "not_configured"}
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    try:
        r = http("POST", f"http://{host}:{port}/",
                 {"jsonrpc": "1.0", "id": "hh", "method": "getblockchaininfo", "params": []},
                 {"Authorization": "Basic " + auth})
        res = r["result"]
        return {"state": "syncing" if res.get("initialblockdownload") else "synced",
                "height": res.get("blocks"), "progress": res.get("verificationprogress")}
    except Exception as e:
        return {"state": "unreachable", "error": str(e)[:100]}


# ---------------------------------------------------------------- alerts

def _post(url, data, headers):
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=8) as r:
        r.read(1000)


def send_webhook(url, fmt, title, msg):
    """ntfy takes a plain body (ASCII title header); the others take JSON."""
    title = title.encode("ascii", "replace").decode()
    if fmt == "ntfy":
        _post(url, msg.encode(), {"Title": title, "Tags": "pickaxe"})
    elif fmt == "discord":
        _post(url, json.dumps({"content": f"**{title}**\n{msg}"}).encode(), {"Content-Type": "application/json"})
    elif fmt == "slack":
        _post(url, json.dumps({"text": f"*{title}*\n{msg}"}).encode(), {"Content-Type": "application/json"})
    else:
        _post(url, json.dumps({"title": title, "message": msg, "time": int(time.time())}).encode(),
              {"Content-Type": "application/json"})


def notify(title, msg):
    """Always lands in the dashboard activity log; also goes to the webhook if one is set."""
    log(f"ALERT {title}: {msg}")
    url, fmt = CFG["alerts"].get("webhook"), CFG["alerts"].get("format", "ntfy")
    if not url:
        return

    def run():
        try:
            send_webhook(url, fmt, title, msg)
        except Exception as e:
            log(f"webhook delivery failed: {str(e)[:100]}")
    threading.Thread(target=run, daemon=True).start()


def edge(key, active, delay, down, up):
    """Fire down() once after `active` has held for `delay` seconds; fire up() once when it clears.
    down/up take the elapsed seconds and return (title, message)."""
    s = ALERT_STATE.setdefault(key, {"since": None, "fired": False})
    now = time.time()
    if active:
        if s["since"] is None:
            s["since"] = now
        if not s["fired"] and now - s["since"] >= delay:
            s["fired"] = True
            notify(*down(now - s["since"]))
    else:
        if s["fired"]:
            notify(*up(now - s["since"]))
        s["since"], s["fired"] = None, False


def mins(sec):
    return f"{sec / 60:.0f} min" if sec >= 90 else f"{sec:.0f} s"


def check_alerts():
    delay = int(CFG["alerts"].get("offline_after_s", 120))
    with LOCK:
        names = {m["name"] for m in CFG["miners"]}
        for k in [k for k in ALERT_STATE if k[0] != "node" and k[1] not in names]:
            del ALERT_STATE[k]
        for m in CFG["miners"]:
            n, st = m["name"], STATUS.get(m["name"])
            if st is None or time.time() - LAST_SWITCH.get(n, 0) < SWITCH_GRACE:
                continue
            online = bool(st.get("online"))
            edge(("offline", n), not online, delay,
                 lambda d, n=n, m=m, st=st: (f"Miner offline: {n}",
                                             f"{n} ({m['host']}) has been unreachable for {mins(d)}. {st.get('error', '')}".strip()),
                 lambda d, n=n: (f"Miner back online: {n}", f"{n} is reachable again after {mins(d)}."))
            edge(("idle", n), online and (st.get("hashrate_ths") or 0) == 0, delay,
                 lambda d, n=n: (f"Miner not hashing: {n}",
                                 f"{n} is online but reports 0 TH/s for {mins(d)}. Its stratum server or node may be down."),
                 lambda d, n=n: (f"Miner hashing again: {n}", f"{n} is hashing again after {mins(d)}."))
            edge(("fallback", n), online and bool(st.get("using_fallback")), min(30, delay),
                 lambda d, n=n: (f"Fallback pool active: {n}",
                                 f"{n} switched to its fallback pool, so your primary stratum server is failing."),
                 lambda d, n=n: (f"Primary pool restored: {n}", f"{n} is back on its primary pool."))
        for cid, c in COINS.items():
            nd = NODES.get(cid)
            if nd is not None and c.get("node_env"):
                edge(("node", cid), nd.get("state") == "unreachable", delay,
                     lambda d, c=c: (f"{c['ticker']} node unreachable", f"HashHub cannot reach the {c['name']} node ({mins(d)})."),
                     lambda d, c=c: (f"{c['ticker']} node reachable", f"The {c['name']} node is reachable again."))


def active_alerts():
    return [{"kind": k[0], "name": k[1]} for k, s in ALERT_STATE.items() if s["fired"]]


# ---------------------------------------------------------------- history

def _num(x):
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def history_db():
    DATA.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DATA / "history.db", timeout=5)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS samples(ts INTEGER NOT NULL, miner TEXT NOT NULL, online INTEGER NOT NULL,"
              " hashrate_ths REAL, power_w REAL, temp REAL, vr_temp REAL, accepted INTEGER, rejected INTEGER, coin TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS samples_miner_ts ON samples(miner, ts)")
    return c


def record_samples(now=None):
    """One row per miner per SAMPLE_SECONDS. Offline counts as 0 TH/s and 0 W so totals and uptime stay honest."""
    now = int(now or time.time())
    rows = []
    with LOCK:
        for m in CFG["miners"]:
            n, st = m["name"], STATUS.get(m["name"])
            if st is None or now - LAST_REC.get(n, 0) < SAMPLE_SECONDS:
                continue
            LAST_REC[n] = now
            if st.get("online"):
                rows.append((now, n, 1, _num(st.get("hashrate_ths")) or 0.0, _num(st.get("power_w")) or 0.0,
                             _num(st.get("temp")), _num(st.get("vr_temp")), st.get("accepted") if isinstance(st.get("accepted"), int) else None,
                             st.get("rejected") if isinstance(st.get("rejected"), int) else None, st.get("coin")))
            else:
                rows.append((now, n, 0, 0.0, 0.0, None, None, None, None, None))
    if not rows:
        return
    with DB_LOCK, closing(history_db()) as c, c:
        c.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        if now - LAST_PRUNE[0] > 6 * 3600:
            c.execute("DELETE FROM samples WHERE ts < ?", (now - HISTORY_DAYS * 86400,))
            LAST_PRUNE[0] = now


def purge_history(name):
    try:
        with DB_LOCK, closing(history_db()) as c, c:
            c.execute("DELETE FROM samples WHERE miner = ?", (name,))
    except sqlite3.Error as e:
        log(f"could not delete history for {name}: {e}")


def history(rng, metric):
    if rng not in RANGES:
        raise HubError("range must be one of " + ", ".join(RANGES))
    if metric not in METRICS:
        raise HubError("metric must be one of " + ", ".join(METRICS))
    span, bucket = RANGES[rng]
    now = int(time.time())
    start = (now - span) // bucket * bucket
    buckets = list(range(start, now + 1, bucket))
    pos = {b: i for i, b in enumerate(buckets)}
    with LOCK:
        names = [m["name"] for m in CFG["miners"]]
    out = {"range": rng, "metric": metric, "bucket_s": bucket, "now": now, "buckets": buckets,
           "retention_days": HISTORY_DAYS, "miners": {n: [None] * len(buckets) for n in names},
           "total": None, "coins": {}, "stats": {}}
    if not names:
        return out
    col, marks = METRICS[metric], ",".join("?" * len(names))
    b_sql = f"ts/{bucket}*{bucket}"
    with DB_LOCK, closing(history_db()) as c:
        rows = c.execute(f"SELECT miner, {b_sql}, coin, SUM(COALESCE({col},0)), COUNT(*), SUM({col} IS NOT NULL) "
                         f"FROM samples WHERE ts >= ? AND miner IN ({marks}) GROUP BY miner, {b_sql}, coin",
                         [start, *names]).fetchall()
        stat_rows = c.execute(f"SELECT miner, COUNT(*), SUM(online), SUM(hashrate_ths), MAX(hashrate_ths) "
                              f"FROM samples WHERE ts >= ? AND miner IN ({marks}) GROUP BY miner",
                              [start, *names]).fetchall()
    agg = {}  # (miner, bucket) -> [sum, samples, non-null samples]
    for miner, b, coin, s, n, nn in rows:
        a = agg.setdefault((miner, b), [0.0, 0, 0])
        a[0] += s; a[1] += n; a[2] += nn
    totals = [None] * len(buckets)
    for (miner, b), (s, n, nn) in agg.items():
        i = pos.get(b)
        if i is None:
            continue
        v = (s / nn if nn else None) if metric == "temp" else s / n  # temp averages only readings that exist
        if v is None:
            continue
        out["miners"][miner][i] = round(v, 3)
        if metric != "temp":
            totals[i] = (totals[i] or 0.0) + v
    if metric != "temp":
        out["total"] = [None if v is None else round(v, 3) for v in totals]
    if metric == "hashrate":  # a miner that switched coin mid-bucket is split in proportion to its samples
        for miner, b, coin, s, n, nn in rows:
            i = pos.get(b)
            if coin and i is not None:
                arr = out["coins"].setdefault(coin, [None] * len(buckets))
                arr[i] = round((arr[i] or 0.0) + s / agg[(miner, b)][1], 3)
    for miner, n, on, s, peak in stat_rows:
        out["stats"][miner] = {"samples": n, "uptime_pct": round(100.0 * on / n, 1),
                               "avg_ths": round((s or 0.0) / n, 3), "peak_ths": round(peak or 0.0, 3)}
    return out


def poll_loop():
    pool = ThreadPoolExecutor(8)
    last_node = 0
    last_db_error = ""
    while True:
        with LOCK:
            miners = list(CFG["miners"])
        list(pool.map(poll_miner, miners))
        if time.time() - last_node > 30:
            for c in list(COINS.values()):
                NODES[c["id"]] = node_status(c)
            last_node = time.time()
        try:
            check_alerts()
        except Exception as e:
            log(f"alert check error: {e}")
        try:
            record_samples()
            last_db_error = ""
        except Exception as e:  # a full or read-only disk must never stop polling or alerts
            if str(e) != last_db_error:
                log(f"history not recorded: {str(e)[:100]}")
            last_db_error = str(e)
        time.sleep(POLL_SECONDS)


# ---------------------------------------------------------------- switching

def switch_miner(m, coin_id):
    coin = COINS.get(coin_id)
    if not coin:
        raise HubError("unknown coin")
    if m.get("algo", "sha256") != coin["algo"]:
        raise HubError(f"{m['name']} mines {m.get('algo', 'sha256')}, {coin['name']} needs {coin['algo']}")
    addr = CFG["addresses"].get(coin_id)
    if not addr:
        raise HubError(f"set a {coin['ticker']} payout address first")
    host = CFG["stratum_host"]
    base = f"http://{m['host']}"
    worker = worker_name(m["name"])
    payload = {"stratumURL": host, "stratumPort": coin["stratum_port"],
               "stratumUser": f"{addr}.{worker}", "stratumPassword": "x"}
    fb = CFG["fallbacks"].get(coin_id)
    if fb:
        payload.update(fallbackStratumURL=fb["host"], fallbackStratumPort=int(fb["port"]),
                       fallbackStratumUser=f"{fb.get('user') or addr}.{worker}", fallbackStratumPassword="x")
    else:  # overwrite any fallback left over from a previous coin so it can't mine the wrong chain
        payload.update(fallbackStratumURL="", fallbackStratumUser="")
    LAST_SWITCH[m["name"]] = time.time()
    try:
        http("PATCH", base + "/api/system", payload)
    except Exception as e:
        raise HubError(f"{m['name']}: could not write pool settings: {e}", 502)
    try:  # AxeOS applies pool changes after a restart; the device drops the connection while rebooting
        http("POST", base + "/api/system/restart", {})
    except urllib.error.HTTPError as e:
        raise HubError(f"{m['name']}: settings saved but restart failed (HTTP {e.code}); restart it by hand", 502)
    except (urllib.error.URLError, OSError):
        pass
    m["coin"] = coin_id
    save_cfg()
    log(f"{m['name']} -> {coin['name']} ({host}:{coin['stratum_port']})"
        + (f", fallback {fb['host']}:{fb['port']}" if fb else ""))


def apply_all():
    """Re-push pool + fallback settings to every online miner on a known coin (restarts them)."""
    with LOCK:
        todo = [(m, STATUS[m["name"]]["coin"]) for m in CFG["miners"]
                if STATUS.get(m["name"], {}).get("online") and STATUS[m["name"]].get("coin")]
    done, errors = 0, []
    for m, coin in todo:
        try:
            switch_miner(m, coin)
            done += 1
        except HubError as e:
            errors.append(str(e))
    if errors:
        raise HubError(f"updated {done}, failed: " + "; ".join(errors), 502)
    return {"updated": done}


def hm(s):
    h, mi = s.split(":")
    return int(h) * 60 + int(mi)


def in_window(start, end, now):
    s, e = hm(start), hm(end)
    if s == e:
        return False
    return s <= now < e if s < e else (now >= s or now < e)


def desired_coins():
    """Schedules apply in order; later ones win. Without schedules nothing is forced."""
    lt = time.localtime()
    now = lt.tm_hour * 60 + lt.tm_min
    want = {}
    for s in CFG["schedules"]:
        coin = s["coin"] if in_window(s["start"], s["end"], now) else s.get("default_coin")
        if not coin:
            continue
        for m in CFG["miners"]:
            if s["miners"] == "*" or m["name"] in s["miners"]:
                want[m["name"]] = coin
    return want


def scheduler_loop():
    while True:
        time.sleep(30)
        try:
            todo = []
            with LOCK:  # decide under the lock, do the slow network calls outside it
                want = desired_coins()
                for m in CFG["miners"]:
                    coin = want.get(m["name"])
                    st = STATUS.get(m["name"], {})
                    if (coin and st.get("online") and st.get("coin") != coin
                            and time.time() - LAST_SWITCH.get(m["name"], 0) > RETRY_SECONDS):
                        todo.append((m, coin))
            for m, coin in todo:
                try:
                    switch_miner(m, coin)
                except HubError as e:
                    log(f"schedule: {e}")
        except Exception as e:
            log(f"scheduler error: {e}")


# ---------------------------------------------------------------- API actions

def state():
    with LOCK:
        miners = [{**m, **STATUS.get(m["name"], {"online": None})} for m in CFG["miners"]]
        totals = {"hashrate_ths": 0.0, "power_w": 0.0, "by_coin": {}}
        for m in miners:
            if m.get("online"):
                totals["hashrate_ths"] += m.get("hashrate_ths") or 0
                totals["power_w"] += m.get("power_w") or 0
                if m.get("coin"):
                    totals["by_coin"][m["coin"]] = round(
                        totals["by_coin"].get(m["coin"], 0) + (m.get("hashrate_ths") or 0), 3)
        totals["hashrate_ths"] = round(totals["hashrate_ths"], 3)
        totals["power_w"] = round(totals["power_w"], 1)
        return {"coins": list(COINS.values()), "miners": miners, "totals": totals,
                "nodes": dict(NODES), "schedules": CFG["schedules"],
                "stratum_host": CFG["stratum_host"], "addresses": CFG["addresses"],
                "fallbacks": CFG["fallbacks"], "coin_slots": CFG["coin_slots"],
                # the webhook URL can embed a secret token, so only its host is ever sent to the browser
                "alerts": {"format": CFG["alerts"]["format"], "offline_after_s": CFG["alerts"]["offline_after_s"],
                           "webhook_host": urlparse(CFG["alerts"]["webhook"]).hostname if CFG["alerts"]["webhook"] else None,
                           "formats": ALERT_FORMATS, "active": active_alerts()},
                "events": EVENTS[-30:], "miner_types": MINER_TYPES}


def set_config(body):
    with LOCK:
        host = body.get("stratum_host")
        if host is not None:
            if not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", host):
                raise HubError("invalid stratum host")
            CFG["stratum_host"] = host
        for cid, addr in (body.get("addresses") or {}).items():
            coin = COINS.get(cid)
            if not coin:
                raise HubError(f"unknown coin {cid}")
            addr = (addr or "").strip()
            if not addr:
                CFG["addresses"].pop(cid, None)
            elif coin.get("address_regex") and not re.fullmatch(coin["address_regex"], addr):
                raise HubError(f"{coin['ticker']} address looks wrong; expected {coin.get('address_hint', 'a valid address')}")
            else:
                CFG["addresses"][cid] = addr
        for cid, fb in (body.get("fallbacks") or {}).items():
            if cid not in COINS:
                raise HubError(f"unknown coin {cid}")
            fb = fb or {}
            host = (fb.get("host") or "").strip()
            if not host:
                CFG["fallbacks"].pop(cid, None)
                continue
            host = re.sub(r"^(stratum\+tcp|tcp)://", "", host)
            port, user = str(fb.get("port") or "").strip(), (fb.get("user") or "").strip()
            if not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", host):
                raise HubError(f"{COINS[cid]['ticker']} fallback host is invalid (hostname only, no path)")
            if not (port.isdigit() and 1 <= int(port) <= 65535):
                raise HubError(f"{COINS[cid]['ticker']} fallback port must be 1-65535")
            if user and not re.fullmatch(r"[A-Za-z0-9:_-]{1,120}", user):
                raise HubError(f"{COINS[cid]['ticker']} fallback username has unsupported characters")
            CFG["fallbacks"][cid] = {"host": host, "port": int(port), "user": user}
        al = body.get("alerts") or {}
        if al.get("clear_webhook"):
            CFG["alerts"]["webhook"] = ""
        elif al.get("webhook"):
            u = urlparse(al["webhook"].strip())
            if u.scheme not in ("http", "https") or not u.hostname or len(al["webhook"]) > 500:
                raise HubError("webhook must be an http(s) URL")
            if u.hostname.startswith("169.254."):
                raise HubError("link-local webhook addresses are not allowed")
            CFG["alerts"]["webhook"] = al["webhook"].strip()
        if al.get("format") is not None:
            if al["format"] not in ALERT_FORMATS:
                raise HubError("unknown alert format")
            CFG["alerts"]["format"] = al["format"]
        if al.get("offline_after_s") is not None:
            try:
                v = int(al["offline_after_s"])
            except (TypeError, ValueError):
                raise HubError("offline_after_s must be a number")
            if not 30 <= v <= 3600:
                raise HubError("alert delay must be 30-3600 seconds")
            CFG["alerts"]["offline_after_s"] = v
        save_cfg()


def test_alert():
    url, fmt = CFG["alerts"].get("webhook"), CFG["alerts"].get("format", "ntfy")
    if not url:
        raise HubError("save a webhook URL first")
    try:
        send_webhook(url, fmt, "HashHub test alert", "If you can read this, alerts are working.")
    except Exception as e:
        raise HubError(f"webhook delivery failed: {str(e)[:120]}", 502)


def add_miner(body):
    name, host = (body.get("name") or "").strip(), (body.get("host") or "").strip()
    mtype = body.get("type", "axeos")
    if not re.fullmatch(r"[A-Za-z0-9 _-]{1,32}", name):
        raise HubError("name: 1-32 letters, digits, space, - or _")
    if not valid_host(host):
        raise HubError("host must be a LAN address (192.168.x.x, 10.x.x.x, name.local)")
    if mtype not in MINER_TYPES:
        raise HubError("unknown miner type")
    with LOCK:
        if any(m["name"] == name for m in CFG["miners"]):
            raise HubError("a miner with that name exists")
        CFG["miners"].append({"name": name, "host": host, "type": mtype, "algo": body.get("algo", "sha256"),
                              "slot": next_slot({m.get("slot") for m in CFG["miners"]})})
        save_cfg()
    log(f"added miner {name} ({host})")


def remove_miner(name):
    with LOCK:
        m = find_miner(name)
        CFG["miners"].remove(m)
        STATUS.pop(name, None)
        LAST_REC.pop(name, None)
        for s in CFG["schedules"]:
            if isinstance(s["miners"], list) and name in s["miners"]:
                s["miners"].remove(name)
        save_cfg()
    purge_history(name)  # its color slot is free again, so its old history must not reappear under a new miner


def add_schedule(body):
    coin, default = body.get("coin"), body.get("default_coin") or None
    for c in (coin, default):
        if c and c not in COINS:
            raise HubError("unknown coin")
    if not coin:
        raise HubError("coin required")
    for k in ("start", "end"):
        if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", body.get(k) or ""):
            raise HubError("start/end must be HH:MM (24h)")
    miners = body.get("miners", "*")
    if miners != "*":
        if not isinstance(miners, list) or not all(isinstance(x, str) for x in miners):
            raise HubError("miners must be '*' or a list of names")
        for n in miners:
            find_miner(n)
    with LOCK:
        CFG["schedules"].append({"id": uuid.uuid4().hex[:8], "miners": miners, "coin": coin,
                                 "default_coin": default, "start": body["start"], "end": body["end"]})
        save_cfg()


def remove_schedule(sid):
    with LOCK:
        CFG["schedules"] = [s for s in CFG["schedules"] if s["id"] != sid]
        save_cfg()


def scan(cidr):
    try:
        net = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        raise HubError("invalid network, use e.g. 192.168.1.0/24")
    if not net.is_private or net.num_addresses > 1024:
        raise HubError("scan a private range no larger than /22")

    def probe(ip):
        try:
            info = http("GET", f"http://{ip}/api/system/info", timeout=1.5)
        except Exception:
            return None
        if isinstance(info, dict) and ("ASICModel" in info or "hashRate" in info):
            return {"host": str(ip), "hostname": info.get("hostname"),
                    "model": info.get("ASICModel") or info.get("deviceModel")}
        return None

    with ThreadPoolExecutor(64) as ex:
        return [r for r in ex.map(probe, net.hosts()) if r]


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = "HashHub"

    def log_message(self, *a):
        pass

    def _send(self, code, obj, ctype="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            return self._send(200, (APP_DIR / "static" / "index.html").read_bytes(), "text/html; charset=utf-8")
        if u.path == "/api/state":
            return self._send(200, state())
        if u.path == "/api/history":
            q = parse_qs(u.query)
            try:
                return self._send(200, history(q.get("range", ["24h"])[0], q.get("metric", ["hashrate"])[0]))
            except HubError as e:
                return self._send(e.status, {"error": str(e)})
            except Exception as e:
                log(f"history query failed: {e}")
                return self._send(500, {"error": "history unavailable"})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        self._mutate("POST")

    def do_DELETE(self):
        self._mutate("DELETE")

    def _mutate(self, method):
        try:
            # A custom header forces a CORS preflight, which blocks cross-site form posts.
            if self.headers.get("X-HashHub") != "1":
                raise HubError("missing X-HashHub header", 403)
            n = int(self.headers.get("Content-Length") or 0)
            if n > 65536:
                raise HubError("body too large", 413)
            body = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(body, dict):
                raise HubError("body must be a JSON object")
            result = self._route(method, self.path, body)
            self._send(200, result if result is not None else {"ok": True})
        except HubError as e:
            self._send(e.status, {"error": str(e)})
        except ValueError:
            self._send(400, {"error": "invalid JSON"})
        except Exception as e:
            log(f"internal error: {e}")
            self._send(500, {"error": "internal error"})

    def _route(self, method, path, body):
        if method == "POST":
            if path == "/api/config":
                return set_config(body)
            if path == "/api/miners":
                return add_miner(body)
            if path == "/api/schedules":
                return add_schedule(body)
            if path == "/api/scan":
                return {"found": scan(body.get("cidr", ""))}
            if path == "/api/alerts/test":
                return test_alert()
            if path == "/api/apply":
                return apply_all()
            mt = re.fullmatch(r"/api/miners/([^/]+)/coin", path)
            if mt:
                with LOCK:
                    m = find_miner(unquote(mt.group(1)))
                switch_miner(m, body.get("coin"))
                return None
        if method == "DELETE":
            mt = re.fullmatch(r"/api/miners/([^/]+)", path)
            if mt:
                return remove_miner(unquote(mt.group(1)))
            mt = re.fullmatch(r"/api/schedules/([0-9a-f]{8})", path)
            if mt:
                return remove_schedule(mt.group(1))
        raise HubError("not found", 404)


def main():
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    log(f"HashHub listening on :{PORT}; coins: {', '.join(COINS)}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
