# HashHub: Umbrel community app store

HashHub is a control plane for home SHA-256 miners on umbrelOS. It does **not** mine and does **not** include a stratum server. It watches your miners, and repoints each one between the solo stratum servers you already run (Bitcoin, Bitcoin Cash, anything you add), by hand or on a schedule.

```
Bitcoin app (Core)  ->  Public Pool / ckpool   :2018   <--+
Bitcoin Cash Node   ->  SoloBCH Forge          :3334   <--+-- Nexus S1, Bitaxe 703, Harlo B30
                                                              ^
                         HashHub (this app, :8420) -----------+  polls /api/system/info,
                                                                  PATCHes pool settings, restarts
```

## Your hardware

| Miner | Chips | Rated | Firmware / API |
|---|---|---|---|
| Nexus S1 | 4x BM1373 | ~10 TH/s, ~100-150 W | AxeOS / ESP-Miner family (open source) |
| Bitaxe Supra Hex 703 | 6x BM1368 | ~4.2 TH/s | AxeOS / ESP-Miner |
| Harlo B30 | 6x BM1373 | 18-30 TH/s, 230-485 W | Harlo-OS (**API not verified**, see below) |

All three are SHA-256, so each can mine BTC or BCH. Fleet total is roughly 44 TH/s and 600-750 W at the wall.

## Install

1. **Build the image.** Push this repo to GitHub, create a tag (`git tag v0.3.0 && git push --tags`) and let the included Action publish `ghcr.io/<you>/hashhub-mining-hub:0.3.0`. Make the package public in GitHub package settings.
2. **Edit placeholders.** Replace `YOUR_GITHUB_USER` / `YOUR_NAME` in `hashhub-mining-hub/umbrel-app.yml` and `docker-compose.yml`. Set `TZ` in the compose file if you are not on US Eastern.
3. **Add the store.** On Umbrel: App Store, the "..." menu, Community App Stores, paste your repo URL.
4. **Install prerequisites** from the Umbrel store: Bitcoin Node, a BTC solo stratum (Public Pool or ckpool), Bitcoin Cash Node, and SoloBCH Forge (community store). Let both nodes finish syncing.
5. **Install HashHub**, open it, then: set the stratum host to your Umbrel's **LAN IP**, enter your BTC and BCH payout addresses, scan your network for miners (or add them by IP), and pick a coin per miner.

## Things you must check on your own setup

- **Stratum ports.** Defaults are 2018 for BTC (Public Pool) and 3334 for BCH (SoloBCH Forge). If your stratum app uses another port, copy the manifest to `<app data>/data/coins/bitcoin.json` and edit it.
- **Pool changes need a restart.** HashHub writes `stratumURL/Port/User` via `PATCH /api/system` and then calls `POST /api/system/restart`, which matches the AxeOS API as documented. Test on the Bitaxe first.
- **Harlo B30.** Harlo-OS is new and I could not find its API documented. HashHub tries the AxeOS-style endpoints on it and shows it as "experimental". If it does not answer, change its pool in its own web UI; HashHub just won't control it.
- **Payout addresses are checked by pattern only,** not by checksum. Verify them yourself.
- **No authentication of its own.** Access is protected by Umbrel's login through the app proxy. Do not expose port 8420 elsewhere.

## Fallback pools

Under "Pool & payout settings" you can set one fallback per coin (host, port, optional username). HashHub writes it to the miner's fallback slot every time it switches that coin, and clears the slot when a coin has no fallback, so a BTC fallback can never be left behind on a miner that is now mining BCH.

- A fallback must be a pool for the **same coin**. HashHub cannot check this for you.
- If you leave the username empty it reuses your payout address for that coin, which is what ckpool-style solo pools expect. Account-style pools need their own username.
- Saving does not touch running miners. Use "Push to miners now" (restarts them) or switch their coin.
- The dashboard marks a miner "on fallback pool" when the miner reports it is using its fallback.

## Alerts

Set a webhook under "Alerts": ntfy (`https://ntfy.sh/<secret-topic>` or your own server), Discord, Slack, or generic JSON. You get one message when a condition has lasted longer than the delay (default 120 s, 30-3600), and one when it clears:

| Alert | Fires when |
|---|---|
| Miner offline | HashHub can't reach the miner |
| Miner not hashing | miner answers but reports 0 TH/s (stratum or node likely down) |
| Fallback pool active | miner switched to its fallback, so your primary stratum is failing |
| Node unreachable | HashHub can't reach a node's RPC (Bitcoin only until you add `node_env` for BCH) |

A miner you just switched gets a 90 s grace period so its reboot doesn't alert. Every alert also appears in the Activity log whether or not a webhook is set. The webhook URL is stored on your Umbrel and is never sent back to the browser. Alerts run inside HashHub, so if the Umbrel itself loses power or network you get nothing; use an external uptime check for that.

## History graphs

HashHub records one sample per miner per minute into a small SQLite file (`<app data>/data/history.db`) and keeps 30 days (`HUB_HISTORY_DAYS` to change). The History card charts, for 1 hour, 24 hours, 7 days or 30 days:

- **By miner:** hashrate per miner plus the fleet total (TH/s)
- **By coin:** hashrate per coin, so you can see how much of the fleet was on BTC vs BCH over time
- **Power** (W) and **Temp** (ASIC, per miner)
- A **Table** twin of every chart, and per-miner average / peak TH/s and uptime for the selected range

Notes:
- Long ranges are averaged into buckets (5 min for 24 h, 30 min for 7 d, 2 h for 30 d), so a short dropout can look shallower on the 30-day view than on the 1-hour view.
- Time offline counts as 0 TH/s and 0 W, so averages and totals reflect what the miners really delivered. Temperature just shows a gap.
- Each miner and coin keeps its chart color for life (up to 8 miners are drawn; extra miners still count toward the total).
- Removing a miner deletes its history.
- Hover or touch for exact values, or focus the chart and use the arrow keys.
- History starts when you install 0.3.0. There is nothing to backfill from the miners.
- If the disk is full or read-only, HashHub keeps polling and alerting and logs "history not recorded".

## Adding a coin

Drop a JSON manifest in `<app data>/data/coins/` (see `hub/coins/litecoin.json.example`). A coin needs a `stratum_port`, an `algo`, and an address pattern. HashHub only lets a miner switch to a coin whose algorithm matches the miner's, so a SHA-256 ASIC can never be pointed at a scrypt coin.

## Not in v0.3

Automatic profitability switching, per-miner fallback overrides, alert repeat reminders, exporting history to CSV. Solo mining rarely needs profitability logic, and the schedule feature covers "BCH overnight, BTC by day".

## Tests

`python3 hub/tests/test_hub.py` runs the Hub against a fake AxeOS miner, and `python3 hub/tests/test_history.py` checks the history storage and queries with synthetic data. Neither needs a network.
