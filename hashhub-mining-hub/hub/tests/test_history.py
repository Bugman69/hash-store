"""History backend test with synthetic data. Run: python3 tests/test_history.py"""
import os, sys, tempfile, threading, time, urllib.request, json
from pathlib import Path

tmp = tempfile.mkdtemp()
os.environ.update(HUB_DATA=tmp, HUB_ALLOW_LOOPBACK="1", HUB_PORT="0", HUB_HISTORY_DAYS="30")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app  # noqa: E402


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond: sys.exit(1)


def near(a, b): return a is not None and abs(a - b) < 1e-6


app.CFG["miners"] = [{"name": "A", "host": "10.0.0.1", "type": "axeos", "algo": "sha256"},
                     {"name": "B", "host": "10.0.0.2", "type": "axeos", "algo": "sha256"}]
app.ensure_slots()
check("slots assigned 1 and 2, kept stable", [m["slot"] for m in app.CFG["miners"]] == [1, 2])
check("coins get slots too", app.CFG["coin_slots"]["bitcoin"] and app.CFG["coin_slots"]["bitcoincash"]
      and app.CFG["coin_slots"]["bitcoin"] != app.CFG["coin_slots"]["bitcoincash"])
check("slot freed by removal is reused", app.next_slot({1, 3}) == 2 and app.next_slot(set(range(1, 9))) == 0)

now = int(time.time())
base = (now - 7200) // 300 * 300  # a 5-minute bucket two hours ago, away from the live edge
rows = []
for k in range(5):  # bucket 0: both online on BTC
    rows += [(base + k * 60, "A", 1, 10.0, 100.0, 60.0, 65.0, 1, 0, "bitcoin"), (base + k * 60, "B", 1, 4.0, 40.0, 50.0, 55.0, 1, 0, "bitcoin")]
for k in range(5):  # bucket 1: B offline
    t = base + 300 + k * 60
    rows += [(t, "A", 1, 10.0, 100.0, 61.0, 66.0, 1, 0, "bitcoin"), (t, "B", 0, 0.0, 0.0, None, None, None, None, None)]
for k in range(5):  # bucket 2: B switches BTC -> BCH after 3 samples
    t = base + 600 + k * 60
    rows += [(t, "A", 1, 10.0, 100.0, 60.0, 65.0, 1, 0, "bitcoin"), (t, "B", 1, 4.0, 40.0, 52.0, 57.0, 1, 0, "bitcoin" if k < 3 else "bitcoincash")]
with app.history_db() as c:
    c.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?)", rows)

h = app.history("24h", "hashrate")
i0 = h["buckets"].index(base)
check("bucket grid is evenly spaced", all(b - a == 300 for a, b in zip(h["buckets"], h["buckets"][1:])))
check("per-miner hashrate averaged per bucket", near(h["miners"]["A"][i0], 10) and near(h["miners"]["B"][i0], 4))
check("total is the sum of miners", near(h["total"][i0], 14))
check("offline miner counts as 0 and total drops", near(h["miners"]["B"][i0 + 1], 0) and near(h["total"][i0 + 1], 10))
check("buckets without data stay null (gaps)", h["miners"]["A"][i0 - 1] is None and h["total"][i0 - 1] is None)
check("coin split is proportional to samples in the bucket",
      near(h["coins"]["bitcoin"][i0 + 2], 12.4) and near(h["coins"]["bitcoincash"][i0 + 2], 1.6))
check("coin series add up to the total", near(h["coins"]["bitcoin"][i0], 14)
      and near(h["coins"]["bitcoin"][i0 + 2] + h["coins"]["bitcoincash"][i0 + 2], h["total"][i0 + 2]))
check("uptime and average stats (B was up 10 of 15 samples)", h["stats"]["B"]["uptime_pct"] == 66.7 and h["stats"]["A"]["uptime_pct"] == 100.0
      and abs(h["stats"]["B"]["avg_ths"] - 8 / 3) < 1e-3 and near(h["stats"]["B"]["peak_ths"], 4))

p = app.history("24h", "power")
check("power totals", near(p["total"][i0], 140) and near(p["total"][i0 + 1], 100))
t = app.history("24h", "temp")
check("temperature averages only real readings; offline is a gap, not 0", near(t["miners"]["B"][i0], 50) and t["miners"]["B"][i0 + 1] is None)
check("temperature has no total or coin series", t["total"] is None and t["coins"] == {})

for bad in (("2d", "hashrate"), ("24h", "dogecoin; DROP TABLE samples")):
    try:
        app.history(*bad); ok = False
    except app.HubError as e:
        ok = e.status == 400
    check(f"rejects {bad[0]}/{bad[1][:10]}", ok)

# sampler
app.STATUS["A"] = {"online": True, "hashrate_ths": 9.7, "power_w": 99, "temp": 58, "vr_temp": 60, "accepted": 5, "rejected": 0, "coin": "bitcoin"}
app.STATUS["B"] = {"online": False, "error": "timeout"}
n0 = len(rows)
app.record_samples(now + 0)
app.record_samples(now + 10)  # inside SAMPLE_SECONDS: must not double-write
with app.history_db() as c:
    cnt = c.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    off = c.execute("SELECT online, hashrate_ths, power_w, temp, coin FROM samples WHERE miner='B' ORDER BY ts DESC LIMIT 1").fetchone()
check("sampler writes one row per miner and rate-limits", cnt == n0 + 2)
check("offline sample stored as 0 TH/s, no temp, no coin", off == (0, 0.0, 0.0, None, None))

# retention
with app.history_db() as c:
    c.execute("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?)", (now - 40 * 86400, "A", 1, 1.0, 1.0, 1.0, 1.0, 1, 0, "bitcoin"))
    c.commit()
app.LAST_PRUNE[0] = 0; app.LAST_REC.clear()
app.record_samples(now + 120)
with app.history_db() as c:
    old = c.execute("SELECT COUNT(*) FROM samples WHERE ts < ?", (now - 31 * 86400,)).fetchone()[0]
check("rows older than the retention window are pruned", old == 0)

# removal purges history
app.CFG["miners"] = [m for m in app.CFG["miners"] if m["name"] != "B"]
app.purge_history("B")
with app.history_db() as c:
    check("removing a miner deletes its history", c.execute("SELECT COUNT(*) FROM samples WHERE miner='B'").fetchone()[0] == 0)
check("removed miner no longer in query", "B" not in app.history("24h", "hashrate")["miners"])

# HTTP endpoint
srv = app.ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
threading.Thread(target=srv.serve_forever, daemon=True).start()
base_url = f"http://127.0.0.1:{srv.server_address[1]}"
d = json.loads(urllib.request.urlopen(base_url + "/api/history?range=1h&metric=hashrate").read())
check("GET /api/history works", d["range"] == "1h" and "A" in d["miners"] and len(d["buckets"]) in (61, 62))
try:
    urllib.request.urlopen(base_url + "/api/history?range=bogus"); ok = False
except urllib.error.HTTPError as e:
    ok = e.code == 400
check("GET /api/history rejects bad range with 400", ok)
print("all passed")
