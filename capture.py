#!/usr/bin/env python3
"""
capture.py: Capture the RealSpeed WebSocket protocol automatically.

This script logs into the hub, connects to the trigger-testing WebSocket,
and dumps every frame it receives. Run it once to capture the protocol,
then paste the output back.

Usage:
    sudo pacman -S python-websockets
    python3 capture.py
"""
import asyncio
import json
import ssl
import sys
import time

try:
    import aiohttp
except ImportError:
    print("error: pacman -S python-aiohttp")
    sys.exit(1)

SSL = ssl.create_default_context()
HUB_ID = None  # Will be auto-detected

async def discover_hub(s):
    """Find your hub's unit ID."""
    for base in ["https://realspeed-v4.eu1.sk.thousandeyes.com",
                 "https://realspeed.eu1.sk.thousandeyes.com"]:
        try:
            async with s.get(f"{base}/ip/discovery", ssl=SSL,
                           timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json(content_type=None)
                units = data if isinstance(data, list) else data.get("data", [])
                if units:
                    uid = units[0].get("unit_id") or units[0].get("id")
                    name = units[0].get("base", "")
                    print(f"Found hub: ID={uid}, Type={name}")
                    return uid
        except Exception as e:
            pass
    return None

async def login(s, uid):
    """Get JWT token."""
    url = "https://realspeed.eu1.sk.thousandeyes.com/ip/login"
    async with s.post(url, json={"unit_id": uid}, ssl=SSL,
                      timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json(content_type=None)
        d = data.get("data", data)
        token = d.get("accessToken", "")
        if not token and isinstance(d, dict):
            token = d.get("data", {}).get("accessToken", "")
        return token

async def capture(s, uid, token):
    """Connect to trigger-testing WebSocket and log everything."""
    ws_url = f"wss://realspeed.eu1.sk.thousandeyes.com/trigger-testing?auth={token}"
    print(f"\nConnecting to WebSocket...")
    print(f"URL: {ws_url[:80]}...")
    print("=" * 60)

    try:
        async with s.ws_connect(
            ws_url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
                "Origin": "https://www.samknows.com",
            },
            heartbeat=10,
            timeout=aiohttp.ClientTimeout(total=60),
            ssl=SSL,
        ) as ws:
            print("Connected! Sending start command...\n")

            # Try the start message
            start_msg = {"type": "start", "testType": "download",
                        "panelId": 3, "unitId": uid}
            await ws.send_json(start_msg)
            print(f">>> SENT: {json.dumps(start_msg)}")

            # Log everything that comes back
            t0 = time.time()
            while time.time() - t0 < 30:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                    elapsed = time.time() - t0
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        print(f"\n<<< [{elapsed:.1f}s] TEXT:")
                        print(f"    {msg.data[:2000]}")
                        try:
                            parsed = json.loads(msg.data)
                            print(f"    PARSED: {json.dumps(parsed, indent=2)[:2000]}")
                        except:
                            pass
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        print(f"\n<<< [{elapsed:.1f}s] BINARY: {len(msg.data)} bytes")
                        print(f"    hex: {msg.data[:100].hex()}")
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        print(f"\n<<< [{elapsed:.1f}s] CLOSE: code={msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print(f"\n<<< [{elapsed:.1f}s] ERROR: {ws.exception()}")
                        break
                except asyncio.TimeoutError:
                    continue

            print("\n" + "=" * 60)
            print("Capture complete. Copy all output above and paste it back.")

    except Exception as e:
        print(f"WebSocket error: {e}")

async def main():
    global HUB_ID

    print("RealSpeed WebSocket Protocol Capture")
    print("=" * 60)

    async with aiohttp.ClientSession() as s:
        # Step 1: Find hub
        print("\nStep 1: Discovering hub...")
        uid = await discover_hub(s)
        if not uid:
            print("ERROR: No hub found on your network.")
            print("Make sure you're connected to your Virgin Media network.")
            sys.exit(1)

        # Step 2: Login
        print("\nStep 2: Logging in...")
        token = await login(s, uid)
        if not token:
            print("ERROR: Login failed.")
            sys.exit(1)
        print(f"  Token acquired (expires in ~15 min)")

        # Step 3: Capture WebSocket
        print("\nStep 3: Capturing WebSocket frames...")
        await capture(s, uid, token)

if __name__ == "__main__":
    asyncio.run(main())
