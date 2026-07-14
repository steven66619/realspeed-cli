#!/usr/bin/env python3
"""
realspeed-cli: Virgin Media SamKnows speed test (device).

Fast CLI speed test using the SamKnows measurement infrastructure
that Virgin Media uses for RealSpeed. Measures speed from your device
to the SamKnows test server — no router/hub interaction needed.

Usage:
    realspeed-cli              # Quick test (~7s)
    realspeed-cli --full       # Thorough test (~15s)
    realspeed-cli --json       # JSON output
    realspeed-cli --help

Dependencies:
    Arch Linux:  sudo pacman -S python-aiohttp
    Other:       pip install aiohttp

Output:
    Download: 455 Mbps  Upload: 93.7 Mbps  Ping: 40 ms
"""

import argparse
import asyncio
import json
import os
import ssl
import sys
import time

import aiohttp

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TARGETS_URL = "https://speedtest-api.samknows.com/targets?targetSet=realspeed-1step"
DL_PORT = 6500
UL_PORT = 80
DL_PATH = "/download"
UL_PATH = "/upload"
SSL_CTX = ssl.create_default_context()
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")

DEBUG = False
def dbg(m):
    if DEBUG:
        print(f"[dbg] {m}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Find test server
# ---------------------------------------------------------------------------

async def find_server(s):
    url = f"https://speedtest-api.samknows.com/targets?targetSet=realspeed-1step"
    dbg(f"GET {url}")
    async with s.get(url, ssl=SSL_CTX, timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json(content_type=None)
        host = data[0].get("hostname", "") if isinstance(data, list) and data else ""
        if not host:
            raise RuntimeError("No test server found")
        dbg(f"  server: {host}")
        return host


# ---------------------------------------------------------------------------
# Latency
# ---------------------------------------------------------------------------

async def measure_ping(s, host, probes=5):
    url = f"http://{host}:{DL_PORT}/"
    lats = []
    for i in range(probes):
        try:
            t0 = time.monotonic()
            async with s.get(url, ssl=False,
                             timeout=aiohttp.ClientTimeout(total=3)) as r:
                await r.read()
            lats.append((time.monotonic() - t0) * 1000)
        except Exception:
            pass
    if not lats:
        return -1.0
    lats.sort()
    if len(lats) > 2:
        lats = lats[1:-1]
    return sum(lats) / len(lats)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

async def measure_download(s, host, duration=3, concurrency=4):
    url = f"http://{host}:{DL_PORT}/download"
    end = time.monotonic() + duration
    bts = [0] * concurrency

    async def go(i):
        while time.monotonic() < end:
            try:
                remain = max(0.5, end - time.monotonic())
                async with s.get(url, ssl=False,
                                 headers={"Accept-Encoding": "identity",
                                          "Cache-Control": "no-cache"},
                                 timeout=aiohttp.ClientTimeout(total=remain)) as r:
                    if r.status != 200:
                        break
                    async for chunk in r.content.iter_any():
                        if time.monotonic() >= end:
                            return
                        bts[i] += len(chunk)
            except Exception:
                break

    await asyncio.gather(*(go(i) for i in range(concurrency)),
                         return_exceptions=True)
    elapsed = max(0.001, time.monotonic() - (end - duration))
    return sum(bts) / elapsed


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

async def measure_upload(s, host, duration=3, concurrency=4):
    url = f"http://{host}:{UL_PORT}{UL_PATH}"
    end = time.monotonic() + duration
    bts = [0] * concurrency
    payload = os.urandom(64 * 1024)

    async def go(i):
        while time.monotonic() < end:
            try:
                remain = max(0.5, end - time.monotonic())
                async with s.post(url, data=payload, ssl=False,
                                  headers={"Content-Type": "application/octet-stream"},
                                  timeout=aiohttp.ClientTimeout(total=remain)) as r:
                    await r.read()
                    bts[i] += len(payload)
            except Exception:
                break

    await asyncio.gather(*(go(i) for i in range(concurrency)),
                         return_exceptions=True)
    elapsed = max(0.001, time.monotonic() - (end - duration))
    return sum(bts) / elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser(
        description="realspeed-cli: Virgin Media SamKnows speed test")
    ap.add_argument("--full", action="store_true",
                    help="Thorough test (~15s) instead of quick (~7s)")
    ap.add_argument("--json", action="store_true",
                    help="Output as JSON")
    ap.add_argument("--debug", action="store_true",
                    help="Show debug output on stderr")
    ap.add_argument("--region", default="EU1", help="API region (default: EU1)")
    args = ap.parse_args()

    global DEBUG
    DEBUG = args.debug

    # Test parameters
    dur = 5 if args.full else 3
    conc = 6 if args.full else 4
    probes = 7 if args.full else 5

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30)
    ) as s:
        # Find server
        host = await find_server(s)

        # Run all three tests concurrently
        ping_task = asyncio.create_task(measure_ping(s, host, probes))
        dl_task = asyncio.create_task(measure_download(s, host, dur, conc))
        ul_task = asyncio.create_task(measure_upload(s, host, dur, conc))

        ping = await ping_task
        dl_bps = await dl_task
        ul_bps = await ul_task

    dl_mbps = round(dl_bps * 8 / 1e6, 1)
    ul_mbps = round(ul_bps * 8 / 1e6, 1)

    # Output
    if args.json:
        print(json.dumps({
            "download_mbps": dl_mbps,
            "upload_mbps": ul_mbps,
            "ping_ms": round(ping, 1) if ping >= 0 else None,
        }))
    else:
        def fmt(v):
            if v < 0: return "N/A"
            return f"{v:.0f}" if v >= 100 else f"{v:.1f}"
        def pfmt(v):
            return f"{v:.0f}" if v >= 0 else "N/A"
        print(f"Download: {fmt(dl_mbps)} Mbps  Upload: {fmt(ul_mbps)} Mbps  Ping: {pfmt(ping)} ms")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr)
        sys.exit(1)
