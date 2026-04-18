# multi_company_server.py — Reference Guide

A self-contained Flask dashboard that polls the Meter public API for multiple companies, stores historical data in SQLite, and serves a single-page UI with live-updating tables, charts, maps, and compliance reports.

---

## File Dependencies

| File | Role |
|------|------|
| `multi_company_server.py` | The entire application — Python backend, Flask routes, HTML/CSS/JS all in one file |
| `config.py` | Credentials and target configuration (API token, company slugs, UUIDs) |
| `meter_data.db` | SQLite database, created automatically on first run in the same directory |
| `requirements.txt` | Python package dependencies |

### `config.py` fields

```python
API_URL      = "https://api.meter.com/api/v1/graphql"
API_TOKEN    = "v2.public...."          # Bearer token from Meter dashboard
COMPANIES    = ["meter", "slug-two", "slug-three"]   # All companies to poll
```

Only `API_TOKEN` and `COMPANIES` are required for the server to function. The other fields are used by standalone scripts in the repo.

### `meter_data.db` — SQLite schema

Five tables, all created automatically by `db_init()` on startup:

| Table | What it stores | Retention |
|-------|---------------|-----------|
| `geocode_cache` | Address → lat/lng from Nominatim, keyed by address string | Permanent |
| `network_snapshots` | Per-network device/client counts written every 5 min | 90 days |
| `uplink_quality` | Per-interface uplink quality values written every 5 min | 90 days |
| `location_snapshots` | Per-area client counts from location analytics | 90 days |
| `event_log_history` | Deduplicated network events (type, timestamp, network) | 90 days |

Old rows are pruned by `db_cleanup()`, which runs after every full refresh cycle.

---

## How to Run

### Prerequisites

```bash
cd meterPublicApi
python3 -m venv .venv          # already exists
source .venv/bin/activate
pip install -r requirements.txt
```

### Start the server

```bash
python3 multi_company_server.py
```

The server starts on **port 8081** by default. Override with the `PORT` environment variable:

```bash
PORT=9000 python3 multi_company_server.py
```

Open `http://localhost:8081` in a browser. Data begins loading immediately — the UI updates progressively as each company's fetch steps complete.

---

## Code Workflow

### Startup sequence

```
main block
  ├── db_init()                      → create/migrate SQLite tables
  ├── Thread: background_loop()      → starts polling immediately
  ├── Thread: _loc_fetch()           → prefetches location analytics
  └── app.run(port=8081)             → Flask serves the UI
```

### Background polling loop (`background_loop`)

Runs forever, once every 5 minutes:

```
background_loop()
  ├── fetch_all()
  │     └── for each slug in COMPANIES → Thread: fetch_company(slug)
  │               └── _do_fetch(slug)
  │                     ├── Step 1: networksForCompany         → network list + addresses
  │                     │     commit() → UI shows company immediately
  │                     ├── Step 2: per-network bundle (parallel per network)
  │                     │     networkClients · uplinkPhyInterfaces · eventLog ·
  │                     │     virtualDevices · SSIDs · VLANs · switch list
  │                     │     commit() → tables become populated
  │                     ├── Step 3: networksUplinkQualities     → uplink quality timeseries
  │                     │     commit()
  │                     ├── Step 4: networkUplinkThroughput     → throughput timeseries (batched)
  │                     │     commit()
  │                     ├── Step 5: switchPortStats             → per-port TX/RX bytes (batched)
  │                     ├── Step 6: phyInterfacesForVirtualDevice → switch client map + port config
  │                     └── commit() + db_write_snapshot() + db_write_uplink() + db_write_events()
  ├── db_cleanup()                   → prune rows older than 90 days
  └── Thread: geocode_all_networks() → geocode any new network addresses into SQLite cache
```

**All company threads run in parallel.** GraphQL calls are serialized through `_gql_semaphore` (a `threading.Semaphore(1)`) to avoid rate-limit collisions. Each company's data is written to the shared snapshot incrementally via `commit(slug, new)` so the UI updates progressively without waiting for all steps to finish.

### Rate-limit handling

Every GraphQL response is inspected for `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `X-Complexity-Remaining`, and `X-Complexity-Reset` headers. If either budget falls below its threshold (`PROACTIVE_RL_THRESH = 20`, `PROACTIVE_CX_THRESH = 100`), the next request sleeps until the reset time. HTTP 429 responses read `Retry-After` and back off accordingly. Each request retries up to `MAX_RETRIES = 3` times with exponential backoff on timeout.

### In-memory snapshot

```python
_snapshot: dict = {}   # { slug → { networks, networkClients, uplinkPhyIfaces,
                        #            eventLog, virtualDevices, ssids, vlans,
                        #            switches, switchClientMap, uplinkQuality,
                        #            uplinkThroughput, switchPortStats, fetchErrors } }
```

`get_snapshot()` returns `(_snapshot, last_updated_timestamp)`. The `/api/data` endpoint serves this directly as JSON. The browser polls `/api/data` every 30 seconds and re-renders changed views.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Serves the full single-page dashboard HTML |
| `GET` | `/api/data` | Full snapshot JSON for all companies |
| `POST` | `/api/refresh` | Triggers an immediate out-of-cycle `fetch_all()` |
| `GET` | `/api/geocode` | Returns `{ nets: { uuid: [lat, lng] } }` from DB cache |
| `GET` | `/api/history/network?uuid=X&days=7` | Device/client count history for one network |
| `GET` | `/api/history/uplink?uuid=X&days=7` | 1-min bucketed avg uplink quality for one network |
| `GET` | `/api/history/events?uuid=X&days=7` | Event log history (omit uuid for all networks) |
| `GET` | `/api/location?days=1` | Location analytics history (max 30 days) |
| `POST` | `/api/location/refresh` | Triggers an immediate location analytics fetch |
| `GET` | `/api/pci/networks` | All networks with IDS/IPS config (for PCI wizard) |
| `POST` | `/api/pci/setup` | Fetches VLANs + SSIDs for selected network UUIDs |
| `POST` | `/api/pci/report` | Runs PCI-DSS analysis and returns findings |

---

## UI Navigation

The sidebar organizes views into five groups. All views share a **Company** and **Network** filter in their toolbar (where applicable), plus a live search box and sortable column headers.

### Overview *(default view)*

A summary card per company showing total networks, devices (by type), clients, and uplink status. Each company can be expanded to see per-network rows with device up/down counts broken out by AP, Switch, Controller, and PDU, plus ISP uplink status chips.

Toggle between **Grid** view (cards) and **Map** view (Leaflet map with health-colored markers). Map markers show a popup on hover with device counts and a 0–100 health score. Map loading is two-phase: cached coordinates from SQLite render immediately; any uncached addresses are geocoded progressively via Nominatim.

---

### ISP

**Uplink Quality** — Time-series chart (last 4 hours, 5-min steps) of uplink quality (0–1 normalized) per interface, across selected networks. Color-coded lines per interface.

**Throughput** — Time-series chart of upload and download throughput (Mbps) per uplink interface.

---

### Network

**Clients** — Table of all connected clients: name, IP, MAC, type (Wi-Fi / Wired), signal strength, VLAN, SSID, connected AP or switch name, switch port number, and last-seen time. Filterable by type, VLAN, SSID.

**SSIDs** — Table of all configured SSIDs: name, enabled/disabled, VLAN association.

**VLANs** — Table of all VLANs: name, VLAN ID, gateway, prefix length, assignment protocol, enabled/disabled.

---

### Infrastructure

**Devices** — Table of all virtual devices (APs, switches, controllers, PDUs): name, type, model, online/offline status, uptime.

**Switch Ports** — Table of every physical switch port across all switches: switch name, network, port number, port label, speed, enabled/disabled, uplink/non-uplink, native VLAN, TX/RX bytes, packet counts.

**Event Log** — Table of recent network events: event type, network, company, timestamp. Filterable by event type.

**History** — Per-network historical charts powered by SQLite data:
- Device Health: total vs. online device counts over time
- Clients: total vs. wireless clients over time
- Uplink Quality: averaged uplink quality score over time
- Event History: scrollable table of stored events

Select a network from the dropdown to load its stored history.

---

### Views

**Map** — Full-screen Leaflet map with a company filter dropdown. Each network is a circle marker colored by uplink health (green = good, yellow = fair, red = poor, gray = unknown). Hover to see a popup with network name, address, device counts, and health score.

---

### Reports

**Config Drift** — Side-by-side comparison of two networks' VLAN and SSID configuration. Select Company A / Network A and Company B / Network B. Differences are highlighted in yellow. Rows are expandable to see a full field-by-field comparison table.

**PCI Compliance** — A multi-step wizard:
1. Select one or more networks to audit
2. Tag which VLANs and SSIDs are in scope (CDE)
3. Run the report — evaluates 7 PCI-DSS v4.0 requirements:
   - REQ-1.2.1: Traffic flow documentation (firewall rule inventory)
   - REQ-1.3.1: Inbound traffic restricted (DENY rules present)
   - REQ-1.4.2: CDE VLAN isolation (inter-VLAN pairs)
   - REQ-2.2.1: Default VLAN not used for CDE
   - REQ-4.2.1: Strong crypto on CDE SSIDs (WPA3 > WPA2 > WEP/open)
   - REQ-11.2.1: Rogue AP detection
   - REQ-11.2.2: Honeypot SSID detection
   - REQ-11.6.1: IDS/IPS status
   Each finding shows PASS / WARNING / FAIL / INFO with details and a recommendation.

**Location Analytics** — Per-area client count charts for Meter HQ (hardcoded AP-to-area mapping). Shows how many clients are in each zone (floor 1 and floor 2 areas) over time. Data is fetched on demand and stored in SQLite for up to 30 days of history.

---

## Topbar Controls

| Element | Function |
|---------|----------|
| Company count | Shows how many companies are loaded |
| 🌙 / ☀️ toggle | Switches between dark mode (default) and light mode; preference saved to `localStorage` |
| Pulse indicator | Animated green dot + "Last updated HH:MM:SS" — blinks when data is stale |
| Refresh button *(per-tab)* | Triggers an immediate `/api/refresh` for the full fetch cycle |

---

