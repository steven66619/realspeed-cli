# realspeed-cli

Command-line speed test using the SamKnows infrastructure that Virgin Media uses for RealSpeed. Tests both router (hub) and device speeds from your terminal.

## Disclaimer

> **This is an unofficial, open-source tool for personal use. It is not affiliated with, endorsed by, or supported by Virgin Media or SamKnows. Use at your own risk.**

## Features

- **Router test** — measures hub-to-server speed via the RealSpeed WebSocket protocol
- **Device test** — measures your device's speed directly to SamKnows test servers
- Runs both by default, or individually with flags
- JSON output for scripting
- Zero external dependencies (Go version)

## Install

### Go (recommended)

```
make
sudo make install
```

### Python

```
pip install aiohttp
```

## Usage

```
realspeed-cli                  # Router + device (default)
realspeed-cli --router         # Router only
realspeed-cli --device         # Device only
realspeed-cli --full           # Longer, more thorough test
realspeed-cli --json           # JSON output
realspeed-cli --debug          # Debug output on stderr
```

### Python version

```
python3 realspeed-cli.py       # Device test only
python3 realspeed-cli.py --full
python3 realspeed-cli.py --json
```

## Example output

```
Router: 2026/217 Mbps | Device: 1790/95.6 Mbps | Ping: 50 ms
```

```
{"device_dl_mbps":1790,"device_ul_mbps":95.6,"ping_ms":50,"router_dl_mbps":2026,"router_ul_mbps":217}
```

## Requirements

- Linux (tested on CachyOS/Arch)
- Connected to a Virgin Media Hub 5/5x for router tests
- Go 1.18+ (for building) or Python 3.8+ with aiohttp

## How it works

**Router test:** Discovers the hub on your local network via the SamKnows RealSpeed API, authenticates, then triggers download and upload tests over a WebSocket connection. The hub runs the test directly to SamKnows servers.

**Device test:** Fetches the nearest SamKnows test server, then runs concurrent HTTP download (GET) and upload (POST) tests with 4 parallel connections for 3 seconds (5 with `--full`).

## License

MIT
