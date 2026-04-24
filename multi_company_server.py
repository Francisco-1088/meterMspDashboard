#!/usr/bin/env python3
"""
multi_company_server.py
=======================
Multi-company Meter Network Dashboard.

Fetches data for every slug in config.COMPANIES every 5 minutes.
Each fetch cycle runs 6 steps per company:
  1  networksForCompany            → networks + mailing addresses
  2  Per-network bundle            → clients · uplink ifaces · events ·
                                     virtualDevices · SSIDs · VLANs
  3  networksUplinkQualities       → all networks in one request
  4  networkUplinkThroughput       → bundled via GraphQL aliases
  5  switchPortStats               → batched aliases per switch
  6  phyInterfacesForVirtualDevice → switch client map + port config

Rate / complexity strategy
  - Tracks X-RateLimit-Remaining, X-RateLimit-Reset
  - Tracks X-Complexity-Remaining, X-Complexity-Reset
  - Sleeps proactively when either budget is low
  - On HTTP 429 reads Retry-After and backs off
  - Retries up to MAX_RETRIES with exponential fallback on timeout
"""

import os
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import config
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

API_URL   = config.API_URL
API_TOKEN = config.API_TOKEN
COMPANIES = config.COMPANIES          # ["meter", "meters-internal-beta-dogfood", "meter-events"]

REFRESH_INTERVAL    = 300             # seconds between full refresh cycles
MAX_RETRIES         = 3
PROACTIVE_RL_THRESH = 20              # sleep when request-rate remaining < this
PROACTIVE_CX_THRESH = 100             # sleep when complexity remaining < this
SWITCH_BATCH_SIZE      = 10   # max switches per aliased batch query
THROUGHPUT_BATCH_SIZE  = 20   # max networks per throughput alias batch

_GQL_HEADERS = {
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {API_TOKEN}",
}

# ── Rate-limit + complexity state (shared across all company fetches) ──────────

_rl_remaining: int | None   = None
_rl_reset: datetime | None  = None
_cx_remaining: int | None   = None
_cx_reset: datetime | None  = None
_limits_lock = threading.Lock()
_gql_semaphore = threading.Semaphore(1)  # serialize API calls across parallel company threads


def _parse_rfc1123(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        return parsedate_to_datetime(v)
    except Exception:
        return None


def _update_limits(hdrs) -> None:
    global _rl_remaining, _rl_reset, _cx_remaining, _cx_reset
    with _limits_lock:
        try:
            _rl_remaining = int(hdrs["X-RateLimit-Remaining"])
        except (KeyError, ValueError):
            pass
        _rl_reset = _parse_rfc1123(hdrs.get("X-RateLimit-Reset"))
        try:
            _cx_remaining = int(hdrs["X-Complexity-Remaining"])
        except (KeyError, ValueError):
            pass
        _cx_reset = _parse_rfc1123(hdrs.get("X-Complexity-Reset"))


def _proactive_sleep() -> None:
    """Pause before a request if rate-limit or complexity budget is low."""
    with _limits_lock:
        rl, rl_r = _rl_remaining, _rl_reset
        cx, cx_r = _cx_remaining, _cx_reset
    now = datetime.now(timezone.utc)
    if rl is not None and rl < PROACTIVE_RL_THRESH:
        wait = (max(0.0, (rl_r - now).total_seconds()) + 1.0) if rl_r else 5.0
        print(f"  ⚠ Rate-limit low ({rl} req) — sleeping {wait:.1f}s", flush=True)
        time.sleep(wait)
        return
    if cx is not None and cx < PROACTIVE_CX_THRESH:
        wait = (max(0.0, (cx_r - now).total_seconds()) + 1.0) if cx_r else 15.0
        print(f"  ⚠ Complexity low ({cx} pts) — sleeping {wait:.1f}s", flush=True)
        time.sleep(wait)


# ── GraphQL client ─────────────────────────────────────────────────────────────

def gql(query: str) -> dict:
    """Execute a GraphQL query. Serialized via semaphore so parallel company threads don't race."""
    with _gql_semaphore:
        return _gql_inner(query)


def _gql_inner(query: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        _proactive_sleep()
        try:
            resp = requests.post(API_URL, json={"query": query},
                                 headers=_GQL_HEADERS, timeout=60)
            _update_limits(resp.headers)

            if resp.status_code == 429:
                retry_dt = _parse_rfc1123(resp.headers.get("Retry-After"))
                wait = (max(0.0, (retry_dt - datetime.now(timezone.utc)).total_seconds()) + 1.0
                        if retry_dt else 60.0 * attempt)
                print(f"  HTTP 429 attempt {attempt}/{MAX_RETRIES} — sleeping {wait:.1f}s",
                      flush=True)
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                return {"error": "HTTP 401 Unauthorized — check API_TOKEN", "code": 401}

            if resp.status_code in (400, 422):
                body = {}
                try:
                    body = resp.json()
                except Exception:
                    pass
                msgs = [e.get("message", "") for e in body.get("errors", [])]
                return {"error": f"HTTP {resp.status_code}", "messages": msgs}

            resp.raise_for_status()
            body = resp.json()

            if "errors" in body and body.get("data") is None:
                codes = [e.get("extensions", {}).get("code", "?") for e in body["errors"]]
                msgs  = [e.get("message") or "" for e in body["errors"]]
                return {"error": f"GraphQL {','.join(codes)}", "messages": msgs}

            return body

        except requests.Timeout:
            print(f"  Timeout attempt {attempt}/{MAX_RETRIES}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
        except requests.ConnectionError as exc:
            return {"error": f"Connection error: {exc}"}

    return {"error": f"All {MAX_RETRIES} attempts failed"}


def _alias(prefix: str, uuid: str) -> str:
    return f"{prefix}_{uuid.replace('-', '_')}"


# ── Multi-company cache ────────────────────────────────────────────────────────

_cache: dict        = {}   # {slug → data-dict}
_last_updated: dict = {}   # {slug → ISO timestamp}
_data_lock          = threading.Lock()


def _empty() -> dict:
    return {
        "networks":        [],   # [{UUID, label, slug, mailingAddress}]
        "networkClients":  {},   # {networkUUID → [client]}
        "uplinkQuality":   {},   # {networkUUID → {metadata, values[]}}
        "uplinkThroughput":{},   # {networkUUID → {metadata, values[]}}
        "uplinkPhyIfaces": {},   # {networkUUID → [iface]}
        "virtualDevices":  {},   # {networkUUID → [device]}
        "ssids":           {},   # {networkUUID → [ssid]}
        "vlans":           {},   # {networkUUID → [vlan]}
        "eventLog":        {},   # {networkUUID → {total, events[]}}
        "switches":        {},   # {networkUUID → [{UUID, label, ports[]}]}
        "switchClientMap": {},   # {networkUUID → {mac → {switchLabel, portNumber}}}
        "fetchErrors":     [],
    }


def commit(slug: str, data: dict) -> None:
    global _last_updated
    ts = datetime.now(timezone.utc).isoformat()
    with _data_lock:
        _cache[slug] = data
        _last_updated[slug] = ts
    errs = data.get("fetchErrors", [])
    if errs:
        print(f"  [{slug}] Done — {len(errs)} error(s): {errs[:2]}", flush=True)
    else:
        print(f"  [{slug}] Done — {ts}", flush=True)


def get_snapshot():
    with _data_lock:
        return {s: dict(d) for s, d in _cache.items()}, dict(_last_updated)


# ── Per-company fetch pipeline ─────────────────────────────────────────────────

_in_progress: set  = set()
_progress_lock     = threading.Lock()


def fetch_company(slug: str) -> None:
    with _progress_lock:
        if slug in _in_progress:
            print(f"  [{slug}] Already in progress — skipping", flush=True)
            return
        _in_progress.add(slug)
    try:
        _do_fetch(slug)
    finally:
        with _progress_lock:
            _in_progress.discard(slug)


def fetch_all() -> None:
    threads = []
    for slug in COMPANIES:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] Polling {slug}…", flush=True)
        t = threading.Thread(target=fetch_company, args=(slug,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def background_loop() -> None:
    while True:
        fetch_all()
        db_cleanup()
        threading.Thread(target=geocode_all_networks, daemon=True).start()
        print(f"\nNext refresh in {REFRESH_INTERVAL}s…", flush=True)
        time.sleep(REFRESH_INTERVAL)


def _do_fetch(slug: str) -> None:
    new = _empty()

    # ── Step 1: Networks ───────────────────────────────────────────────────────
    print(f"  [{slug}] [1/6] Networks…", flush=True)
    r = gql(f"""{{
      networksForCompany(companySlug: "{slug}") {{
        UUID label slug
        mailingAddress {{ line1 line2 city postalCode }}
      }}
    }}""")
    if "error" in r:
        new["fetchErrors"].append(f"networks: {r['error']}")
        commit(slug, new)
        return

    networks = (r.get("data") or {}).get("networksForCompany") or []
    new["networks"] = networks
    print(f"  [{slug}] {len(networks)} network(s)", flush=True)
    commit(slug, new)  # make company visible immediately after networks load
    if not networks:
        return

    uuids = [n["UUID"] for n in networks]

    # ── Step 2: Per-network bundle ─────────────────────────────────────────────
    print(f"  [{slug}] [2/6] Per-network bundles ({len(networks)})…", flush=True)
    for net in networks:
        nid   = net["UUID"]
        label = net.get("label", nid)
        print(f"    [{slug}] → {label}", flush=True)

        r = gql(f"""{{
          clients: networkClients(networkUUID: "{nid}") {{
            macAddress ip clientName isWireless signal lastSeen
            connectedVLAN {{ name vlanID }}
            connectedSSID {{ ssid }}
            accessPoint {{ UUID label }}
          }}
          phyIfaces: uplinkPhyInterfacesForNetwork(networkUUID: "{nid}") {{
            UUID label portNumber isEnabled isUplink isUplinkActive portSpeedMbps
            virtualDeviceUUID nativeVLAN {{ name vlanID }}
          }}
          events: recentEventLogEventsPage(networkUUID: "{nid}", limit: 50) {{
            total events {{ eventType eventTypeAPIName generatedAt networkUUID }}
          }}
          devices: virtualDevicesForNetwork(networkUUID: "{nid}") {{
            UUID label deviceType deviceModel isOnline uptime
          }}
          ssids: ssidsForNetwork(networkUUID: "{nid}") {{
            UUID ssid isEnabled
          }}
          vlans: vlans(networkUUID: "{nid}") {{
            UUID name vlanID isEnabled
            ipV4ClientGateway ipV4ClientPrefixLength
            ipV4ClientAssignmentProtocol
          }}
        }}""")

        if "error" in r:
            new["fetchErrors"].append(f"{label}: {r['error']}")
            continue

        d = r.get("data") or {}
        new["networkClients"][nid]  = d.get("clients")  or []
        new["uplinkPhyIfaces"][nid] = d.get("phyIfaces") or []
        new["eventLog"][nid]        = d.get("events")    or {"total": 0, "events": []}
        new["virtualDevices"][nid]  = d.get("devices")   or []
        new["ssids"][nid]           = d.get("ssids")     or []
        new["vlans"][nid]           = d.get("vlans")     or []

        switches = [dv for dv in (d.get("devices") or []) if dv.get("deviceType") == "SWITCH"]
        new["switches"][nid] = [
            {"UUID": sw["UUID"], "label": sw.get("label", sw["UUID"]),
             "networkUUID": nid, "networkLabel": label, "ports": []}
            for sw in switches
        ]

    commit(slug, new)  # devices/clients/SSIDs/VLANs now visible

    # ── Step 3: Uplink quality ─────────────────────────────────────────────────
    print(f"  [{slug}] [3/6] Uplink quality…", flush=True)
    uuids_gql = ", ".join(f'"{u}"' for u in uuids)
    r = gql(f"""{{
      networksUplinkQualities(
        networkUUIDs: [{uuids_gql}],
        filter: {{ durationSeconds: 14400, stepSeconds: 300 }}
      ) {{
        metadata {{ minValue maxValue }}
        values {{ timestamp value phyInterfaceUUID networkUUID }}
      }}
    }}""")
    if "error" not in r:
        for entry in (r.get("data") or {}).get("networksUplinkQualities") or []:
            meta = entry.get("metadata") or {}
            for val in entry.get("values") or []:
                nid = val.get("networkUUID")
                if not nid:
                    continue
                if nid not in new["uplinkQuality"]:
                    new["uplinkQuality"][nid] = {"metadata": meta, "values": []}
                new["uplinkQuality"][nid]["values"].append(val)
    else:
        new["fetchErrors"].append(f"uplinkQuality: {r['error']}")

    commit(slug, new)  # uplink quality now visible

    # ── Step 4: Throughput (batched aliases) ───────────────────────────────────
    print(f"  [{slug}] [4/6] Throughput…", flush=True)
    for i in range(0, len(uuids), THROUGHPUT_BATCH_SIZE):
        batch_uuids = uuids[i : i + THROUGHPUT_BATCH_SIZE]
        parts = [
            f"""  {_alias('tput', u)}: networkUplinkThroughput(
    networkUUID: "{u}",
    filter: {{ durationSeconds: 14400, stepSeconds: 300 }}
  ) {{
    metadata {{ minValue maxValue }}
    values {{ timestamp value direction phyInterfaceUUID }}
  }}"""
            for u in batch_uuids
        ]
        r = gql("{\n" + "\n".join(parts) + "\n}")
        if "error" not in r:
            d = r.get("data") or {}
            for u in batch_uuids:
                a = _alias("tput", u)
                if a in d and d[a]:
                    new["uplinkThroughput"][u] = d[a]
        else:
            new["fetchErrors"].append(f"throughput: {r['error']}")

    commit(slug, new)  # throughput now visible

    # ── Step 5: Switch port stats ──────────────────────────────────────────────
    all_sw = [(nid, sw) for nid, sws in new["switches"].items() for sw in sws]
    if all_sw:
        print(f"  [{slug}] [5/6] Switch port stats ({len(all_sw)} switches)…", flush=True)
        for i in range(0, len(all_sw), SWITCH_BATCH_SIZE):
            batch = all_sw[i : i + SWITCH_BATCH_SIZE]
            parts = [
                f"""  {_alias('sw', sw['UUID'])}: switchPortStats(virtualDeviceUUID: "{sw['UUID']}") {{
    portNumber totalRxBytes totalTxBytes totalRxPackets totalTxPackets
    errorRxPackets errorTxPackets
  }}"""
                for _, sw in batch
            ]
            r = gql("{\n" + "\n".join(parts) + "\n}")
            if "error" not in r:
                d = r.get("data") or {}
                for nid, sw in batch:
                    a = _alias("sw", sw["UUID"])
                    if a in d and d[a]:
                        sw["ports"] = d[a]
            else:
                new["fetchErrors"].append(f"switchPorts: {r['error']}")
    else:
        print(f"  [{slug}] [5/6] No switches", flush=True)

    # ── Step 6: Switch client map + port config ────────────────────────────────
    if all_sw:
        print(f"  [{slug}] [6/6] Switch port config…", flush=True)
        for nid in new["switches"]:
            new["switchClientMap"][nid] = {}
        for i in range(0, len(all_sw), SWITCH_BATCH_SIZE):
            batch = all_sw[i : i + SWITCH_BATCH_SIZE]
            parts = [
                f"""  {_alias('scd', sw['UUID'])}: phyInterfacesForVirtualDevice(virtualDeviceUUID: "{sw['UUID']}") {{
    portNumber label isEnabled isUplink isUplinkActive portSpeedMbps
    nativeVLAN {{ name vlanID }}
    connectedDevices {{ client {{ macAddress }} portNumber }}
  }}"""
                for _, sw in batch
            ]
            r = gql("{\n" + "\n".join(parts) + "\n}")
            if "error" not in r:
                d = r.get("data") or {}
                for nid, sw in batch:
                    a = _alias("scd", sw["UUID"])
                    for iface in (d.get(a) or []):
                        pn = iface.get("portNumber")
                        for cd in (iface.get("connectedDevices") or []):
                            client = cd.get("client")
                            if client and client.get("macAddress"):
                                new["switchClientMap"][nid][client["macAddress"].lower()] = {
                                    "switchLabel": sw.get("label", sw["UUID"]),
                                    "portNumber":  pn,
                                }
                        if pn is not None:
                            for p in sw.get("ports", []):
                                if p.get("portNumber") == pn:
                                    p.update({
                                        "label":         iface.get("label"),
                                        "isEnabled":     iface.get("isEnabled"),
                                        "isUplink":      iface.get("isUplink"),
                                        "portSpeedMbps": iface.get("portSpeedMbps"),
                                        "nativeVLAN":    iface.get("nativeVLAN"),
                                    })
                                    break
            else:
                new["fetchErrors"].append(f"switchClientMap: {r['error']}")
    else:
        print(f"  [{slug}] [6/6] No switches", flush=True)

    commit(slug, new)
    db_write_snapshot(slug, new)
    db_write_uplink(new)
    db_write_events(slug, new)


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/data")
def api_data():
    data, ts = get_snapshot()
    return jsonify({"data": data, "last_updated": ts, "companies": COMPANIES})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=fetch_all, daemon=True).start()
    return jsonify({"status": "refresh started"})


# ── PCI-DSS compliance routes ──────────────────────────────────────────────────

from flask import request as flask_request


def _pci_gql(query: str) -> dict:
    """Thin GQL wrapper reusing the main gql() function."""
    return gql(query)


def _pci_fetch_networks():
    results = []
    for slug in COMPANIES:
        r = _pci_gql(f"""{{
          networksForCompany(companySlug: "{slug}") {{
            UUID label slug
            idsIPSConfig {{ UUID networkUUID mode }}
          }}
        }}""")
        if "error" not in r:
            nets = (r.get("data") or {}).get("networksForCompany") or []
            for n in nets:
                n["_slug"] = slug
                n["_idsIPSConfig"] = n.pop("idsIPSConfig", None)
            results.extend(nets)
    return results


def _pci_fetch_vlans_ssids(nid: str) -> tuple:
    r = _pci_gql(f"""{{
      vlans(networkUUID: "{nid}") {{ UUID name vlanID description isEnabled }}
      ssidsForNetwork(networkUUID: "{nid}") {{
        UUID ssid isEnabled encryptionProtocol
        vlan {{ UUID name vlanID }}
      }}
    }}""")
    if "error" in r:
        return [], [], r["error"]
    d = r.get("data") or {}
    return d.get("vlans") or [], d.get("ssidsForNetwork") or [], None


def _pci_fetch_compliance(nid: str, idsips_cfg) -> dict:
    result = {"errors": [], "idsIPSConfig": idsips_cfg}
    r = _pci_gql(f"""{{
      vlans(networkUUID: "{nid}") {{ UUID name vlanID description isEnabled }}
      ssids: ssidsForNetwork(networkUUID: "{nid}") {{
        UUID ssid isEnabled encryptionProtocol vlan {{ UUID name vlanID }}
      }}
      fwRules: firewallRulesForNetwork(networkUUID: "{nid}") {{
        UUID name description action isEnabled srcPrefix dstPrefix priority
      }}
      ivPairs: interVLANCommunicationPermittedPairs(networkUUID: "{nid}") {{
        UUID networkUUID
      }}
    }}""")
    if "error" in r:
        result["errors"].append(f"Core data: {r['error']}")
        result.update({"vlans": [], "ssids": [], "fwRules": [], "ivPairs": []})
    else:
        d = r.get("data") or {}
        result["vlans"]   = d.get("vlans")   or []
        result["ssids"]   = d.get("ssids")   or []
        result["fwRules"] = d.get("fwRules") or []
        result["ivPairs"] = d.get("ivPairs") or []

    for query_name, result_key in [
        (f'idsEvents(networkUUID: "{nid}", limit: 50, durationSeconds: 86400)', "idsEvents"),
        (f'ipsEvents(networkUUID: "{nid}", limit: 50, durationSeconds: 86400)', "ipsEvents"),
    ]:
        top = query_name.split("(")[0]
        r2 = _pci_gql(f"{{ {query_name} {{ events {{ observedAt srcIP dstIP protocol direction srcPort dstPort virtualDevice {{ UUID label }} }} total }} }}")
        if "error" in r2:
            result["errors"].append(f"{top}: {r2['error']}")
            result[result_key] = {"events": [], "total": 0}
        else:
            raw = (r2.get("data") or {}).get(top) or {}
            result[result_key] = {"events": raw.get("events") or [], "total": raw.get("total") or 0}

    tf = '{ timeFilter: { durationSeconds: 86400, stepSeconds: 300 } }'
    for qname, key in [
        (f'rogueAPs: rogueAccessPointsObservedOnNetwork(networkUUIDs: ["{nid}"], filter: {tf}, limit: 50, offset: 0) {{ networkUUID observers {{ ssid rssi observedAt }} }}', "rogueAPs"),
        (f'honeypots: honeypotSSIDsObservedOnNetwork(networkUUIDs: ["{nid}"], filter: {tf}, limit: 50, offset: 0) {{ networkUUID ssid }}', "honeypots"),
    ]:
        alias = qname.split(":")[0].strip()
        r3 = _pci_gql(f"{{ {qname} }}")
        result[key] = (r3.get("data") or {}).get(alias) or [] if "error" not in r3 else []
        if "error" in r3:
            result["errors"].append(f"{alias}: {r3['error']}")

    return result


def _pci_analyze(data: dict, cde_vlan_uuids: list, cde_ssid_uuids: list) -> dict:
    """Identical logic to pci_report.py analyze()."""
    from datetime import datetime, timezone as tz
    findings = []
    vlan_map = {v["UUID"]: v for v in (data.get("vlans") or [])}
    ssid_map = {s["UUID"]: s for s in (data.get("ssids") or [])}
    cde_vlans = [vlan_map[u] for u in cde_vlan_uuids if u in vlan_map]
    cde_ssids = [ssid_map[u] for u in cde_ssid_uuids if u in ssid_map]
    cde_vlan_names = [v["name"] for v in cde_vlans]
    all_rules = data.get("fwRules") or []
    active_rules = [r for r in all_rules if r["isEnabled"]]
    deny_rules   = [r for r in active_rules if r["action"] == "DENY"]
    permit_rules = [r for r in active_rules if r["action"] == "PERMIT"]

    findings.append({"id":"REQ-1.2.1","req":"PCI DSS v4.0 — Req 1.2.1","title":"Traffic Flow Documentation",
        "status":"INFO","summary":f"{len(all_rules)} rule(s) ({len(deny_rules)} DENY, {len(permit_rules)} PERMIT)",
        "details":[f"[{'ON' if r['isEnabled'] else 'OFF'}][{r['action']}] {r['name'] or 'Unnamed'} src:{r['srcPrefix'] or 'any'} dst:{r['dstPrefix'] or 'any'}" for r in sorted(all_rules,key=lambda x:x.get("priority",0))] or ["No rules"],
        "recommendation":None})

    if not all_rules: fw_s,fw_sum,fw_rec = "FAIL","No firewall rules","Add rules restricting CDE traffic."
    elif not deny_rules: fw_s,fw_sum,fw_rec = "WARNING","No DENY rules","Add DENY rules for CDE."
    else: fw_s,fw_sum,fw_rec = "PASS",f"{len(deny_rules)} DENY / {len(permit_rules)} PERMIT",None
    findings.append({"id":"REQ-1.3.1","req":"PCI DSS v4.0 — Req 1.3.1","title":"Inbound Traffic Restricted",
        "status":fw_s,"summary":fw_sum,"details":[f"Active:{len(active_rules)} Deny:{len(deny_rules)} Permit:{len(permit_rules)}"],"recommendation":fw_rec})

    iv = len(data.get("ivPairs") or [])
    if not cde_vlans: iv_s,iv_sum,iv_rec = "MANUAL","No CDE VLANs","Select CDE VLANs."
    elif iv==0: iv_s,iv_sum,iv_rec = "PASS","No inter-VLAN pairs permitted",None
    else: iv_s,iv_sum,iv_rec = "WARNING",f"{iv} inter-VLAN pair(s)","Verify no CDE VLAN is in a permitted pair."
    findings.append({"id":"REQ-1.4.2","req":"PCI DSS v4.0 — Req 1.4.2","title":"CDE VLAN Isolation",
        "status":iv_s,"summary":iv_sum,"details":[f"CDE: {', '.join(cde_vlan_names)}" if cde_vlan_names else ""],"recommendation":iv_rec})

    def_cde = [v for v in cde_vlans if v["vlanID"]==1]
    findings.append({"id":"REQ-2.2.1","req":"PCI DSS v4.0 — Req 2.2.1","title":"Default VLAN Not Used for CDE",
        "status":"WARNING" if def_cde else ("PASS" if cde_vlans else "MANUAL"),
        "summary":f"Default VLAN in CDE: {def_cde[0]['name']}" if def_cde else ("No default VLAN in CDE" if cde_vlans else "No CDE VLANs"),
        "details":[f"VLAN '{v['name']}' uses ID 1" for v in def_cde] or ["OK"],
        "recommendation":"Move CDE off VLAN ID 1." if def_cde else None})

    STRONG,ACCEPTABLE,WEAK = {"WPA3"},{"WPA2"},{"WEP","WPA"}
    if not cde_ssids: enc_s,enc_sum,enc_det,enc_rec = "MANUAL","No CDE SSIDs selected",["Select SSIDs"],None
    else:
        enc_det=[]; fails=[]; warns=[]
        for s in cde_ssids:
            p=(s.get("encryptionProtocol") or "").upper()
            if not p: enc_det.append(f"✗ {s['ssid']}: Open — FAIL"); fails.append(s['ssid'])
            elif p in WEAK: enc_det.append(f"✗ {s['ssid']}: {p} — FAIL"); fails.append(s['ssid'])
            elif p in ACCEPTABLE: enc_det.append(f"△ {s['ssid']}: {p} — WARNING"); warns.append(s['ssid'])
            else: enc_det.append(f"✓ {s['ssid']}: {p} — PASS")
        if fails: enc_s,enc_sum,enc_rec = "FAIL",f"{len(fails)} SSID(s) weak encryption","Apply WPA2/WPA3."
        elif warns: enc_s,enc_sum,enc_rec = "WARNING","All encrypted; WPA2 used (WPA3 preferred)","Upgrade to WPA3."
        else: enc_s,enc_sum,enc_rec = "PASS",f"All {len(cde_ssids)} CDE SSIDs use WPA3",None
    findings.append({"id":"REQ-4.2.1","req":"PCI DSS v4.0 — Req 4.2.1","title":"Strong Crypto for CDE SSIDs",
        "status":enc_s,"summary":enc_sum,"details":enc_det,"recommendation":enc_rec})

    rogue_obs = [obs for ap in (data.get("rogueAPs") or []) for obs in (ap.get("observers") or [])]
    findings.append({"id":"REQ-11.2.1","req":"PCI DSS v4.0 — Req 11.2.1","title":"Rogue AP Detection",
        "status":"FAIL" if rogue_obs else "PASS",
        "summary":f"{len(rogue_obs)} rogue AP observation(s)" if rogue_obs else "No rogue APs detected",
        "details":[f"SSID:{o.get('ssid','?')} RSSI:{o.get('rssi','?')} @ {str(o.get('observedAt',''))[:19]}" for o in rogue_obs[:10]],
        "recommendation":"Investigate rogue APs." if rogue_obs else None})

    honeypots = data.get("honeypots") or []
    cde_names = {s["ssid"] for s in cde_ssids}
    matching = [h["ssid"] for h in honeypots if h["ssid"] in cde_names]
    findings.append({"id":"REQ-11.2.2","req":"PCI DSS v4.0 — Req 11.2.2","title":"Honeypot SSID Detection",
        "status":"FAIL" if matching else ("WARNING" if honeypots else "PASS"),
        "summary":f"Matching honeypots: {', '.join(matching)}" if matching else (f"{len(honeypots)} honeypot(s) observed" if honeypots else "No honeypots"),
        "details":[h["ssid"] for h in honeypots[:10]],
        "recommendation":"Investigate immediately." if matching else ("Review honeypots." if honeypots else None)})

    ids_total = (data.get("idsEvents") or {}).get("total") or 0
    ips_total = (data.get("ipsEvents") or {}).get("total") or 0
    cfg = data.get("idsIPSConfig")
    mode = cfg.get("mode") if cfg else None
    if not cfg: ids_s,ids_sum,ids_rec = "FAIL","IDS/IPS not configured","Enable IDS/IPS on this network."
    elif mode=="IPS": ids_s,ids_sum,ids_rec = "PASS",f"IPS active — {ids_total} IDS / {ips_total} IPS events","Review events regularly."
    elif mode=="IDS": ids_s,ids_sum,ids_rec = "WARNING",f"IDS only (detection) — {ids_total} events","Upgrade to IPS mode on CDE segments."
    else: ids_s,ids_sum,ids_rec = "INFO",f"IDS/IPS mode:{mode}","Review events regularly."
    findings.append({"id":"REQ-11.5.1","req":"PCI DSS v4.0 — Req 11.5.1","title":"IDS/IPS Deployed",
        "status":ids_s,"summary":ids_sum,
        "details":[f"Mode: {mode or 'N/A'}",f"IDS events: {ids_total}",f"IPS events: {ips_total}"],
        "recommendation":ids_rec})

    counts = {}
    for f in findings:
        counts[f["status"]] = counts.get(f["status"], 0) + 1
    return {"findings": findings, "summary": counts, "generated_at": datetime.now(tz.utc).isoformat()}


@app.route("/api/pci/networks")
def api_pci_networks():
    nets = _pci_fetch_networks()
    return jsonify({"networks": nets})


@app.route("/api/pci/setup", methods=["POST"])
def api_pci_setup():
    body = flask_request.get_json() or {}
    result = {}
    for nid in body.get("networkUUIDs", []):
        vlans, ssids, err = _pci_fetch_vlans_ssids(nid)
        result[nid] = {"vlans": vlans, "ssids": ssids, "error": err}
    return jsonify(result)


@app.route("/api/pci/report", methods=["POST"])
def api_pci_report():
    body = flask_request.get_json() or {}
    report_nets = []
    for net in body.get("networks", []):
        nid = net["uuid"]
        # Try to get idsIPSConfig from cached data if available
        idsips = None
        for slug in COMPANIES:
            cached_nets = (get_snapshot()[0].get(slug) or {}).get("networks") or []
            found = next((n for n in cached_nets if n.get("UUID") == nid), None)
            if found:
                break
        data = _pci_fetch_compliance(nid, idsips)
        result = _pci_analyze(data, net.get("cdeVlanUUIDs", []), net.get("cdeSsidUUIDs", []))
        result["network"] = {"uuid": nid, "label": net.get("label", nid)}
        result["fetchErrors"] = data.get("errors", [])
        report_nets.append(result)
    return jsonify({"networks": report_nets})


# ── Location analytics routes ──────────────────────────────────────────────────

import re as _re

_LOC_AP_AREAS = {
    "1.7-A":"1st Floor Desks","1.8-A":"1st Floor Desks","1.10-A":"1st Floor Desks","1.6-A":"1st Floor Desks",
    "1.3-A":"1st Floor Meeting Rooms","1.4-A":"1st Floor Meeting Rooms","1.5-A":"1st Floor Meeting Rooms",
    "1.1-A":"1st Floor Kitchen & Entrance","1.2-A":"1st Floor Kitchen & Entrance",
    "1.9-A":"1st Floor Meter Labs",
    "2.16-A":"2nd Floor Desks","2.18-A":"2nd Floor Desks","2.17-A":"2nd Floor Desks",
    "2.21-A":"2nd Floor Desks","2.24-A":"2nd Floor Desks","2.15-A":"2nd Floor Desks",
    "2.11-A":"2nd Floor Sales","2.13-A":"2nd Floor Sales",
    "2.23-A":"2nd Floor Meter Cafe","2.22-A":"2nd Floor Meter Cafe",
    "2.14-A":"2nd Floor Meeting Rooms","2.12-A":"2nd Floor Meeting Rooms","2.19-A":"2nd Floor Meeting Rooms",
    "2.20-A":"2nd Floor Network",
}
_LOC_FLOOR1 = ["1st Floor Desks","1st Floor Meeting Rooms","1st Floor Kitchen & Entrance","1st Floor Meter Labs"]
_LOC_FLOOR2 = ["2nd Floor Desks","2nd Floor Sales","2nd Floor Meter Cafe","2nd Floor Meeting Rooms","2nd Floor Network"]
_LOC_ALL_AREAS = _LOC_FLOOR1 + _LOC_FLOOR2

_loc_history: list = []
_loc_last_updated: str | None = None
_loc_last_error: str | None = None
_loc_hist_lock = threading.Lock()
_loc_fetch_lock = threading.Lock()
_loc_fetching = False


def _loc_normalize(raw: str) -> str:
    label = _re.sub(r"\s*\([^)]*\)", "", raw).strip()
    label = _re.sub(r"^0+(\d)", r"\1", label)
    return label


def _loc_fetch() -> None:
    global _loc_last_updated, _loc_last_error, _loc_fetching
    with _loc_fetch_lock:
        if _loc_fetching:
            return
        _loc_fetching = True
    try:
        r = gql(f"""{{
          networksForCompany(companySlug: "{COMPANIES[0]}") {{ UUID label }}
        }}""")
        if "error" in r:
            _loc_last_error = r["error"]; return
        nets = (r.get("data") or {}).get("networksForCompany") or []
        primary = next((n for n in nets if "primary" in (n.get("label") or "").lower()), None) or (nets[0] if nets else None)
        if not primary:
            _loc_last_error = "No networks found"; return
        nid = primary["UUID"]
        r2 = gql(f"""{{ networkClients(networkUUID: "{nid}") {{ macAddress isWireless accessPoint {{ UUID label }} }} }}""")
        if "error" in r2:
            _loc_last_error = r2["error"]; return
        clients = (r2.get("data") or {}).get("networkClients") or []
        ap_counts = {ap: 0 for ap in _LOC_AP_AREAS}
        for c in clients:
            if not c.get("isWireless"): continue
            ap_obj = c.get("accessPoint")
            if isinstance(ap_obj, list): ap_obj = ap_obj[0] if ap_obj else None
            if not ap_obj: continue
            norm = _loc_normalize((ap_obj.get("label") or "").strip())
            if norm in ap_counts: ap_counts[norm] += 1
        area_counts = {a: 0 for a in _LOC_ALL_AREAS}
        for ap, cnt in ap_counts.items():
            area_counts[_LOC_AP_AREAS[ap]] += cnt
        ts = datetime.now(timezone.utc).isoformat()
        db_write_location(ts, area_counts)
        with _loc_hist_lock:
            _loc_history.append({"ts": ts, "ap_counts": ap_counts, "area_counts": area_counts})
            if len(_loc_history) > 288: del _loc_history[:-288]
            _loc_last_updated = ts
            _loc_last_error = None
    finally:
        with _loc_fetch_lock:
            _loc_fetching = False


@app.route("/api/location")
def api_location():
    days = min(int(flask_request.args.get("days", 1)), 30)
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    # Pull DB history for the requested window
    try:
        with _db() as conn:
            db_rows = conn.execute(
                "SELECT ts, area, count FROM location_snapshots WHERE ts>=? ORDER BY ts, area",
                (since,)).fetchall()
        by_ts: dict = defaultdict(dict)
        for r in db_rows:
            by_ts[r["ts"]][r["area"]] = r["count"]
        db_hist = [{"ts": ts, "area_counts": areas} for ts, areas in sorted(by_ts.items())]
    except Exception:
        db_hist = []
    # Merge with in-memory (covers the current 24h window even before DB write)
    with _loc_hist_lock:
        mem_hist = list(_loc_history)
    known_ts = {h["ts"] for h in db_hist}
    merged = db_hist + [h for h in mem_hist if h["ts"] not in known_ts]
    merged.sort(key=lambda h: h["ts"])
    return jsonify({
        "history": merged, "last_updated": _loc_last_updated, "last_error": _loc_last_error,
        "all_areas": _LOC_ALL_AREAS, "floor1_areas": _LOC_FLOOR1, "floor2_areas": _LOC_FLOOR2,
    })


@app.route("/api/location/refresh", methods=["POST"])
def api_location_refresh():
    threading.Thread(target=_loc_fetch, daemon=True).start()
    return jsonify({"status": "started"})


# ── SQLite persistence ─────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "meter_data.db"
_DB_RETENTION_DAYS = 90


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def db_init() -> None:
    with _db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                address TEXT PRIMARY KEY,
                lat REAL,
                lng REAL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS network_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                slug TEXT NOT NULL,
                network_uuid TEXT NOT NULL,
                network_label TEXT NOT NULL,
                devices_total INTEGER DEFAULT 0,
                devices_online INTEGER DEFAULT 0,
                ap_total INTEGER DEFAULT 0, ap_online INTEGER DEFAULT 0,
                switch_total INTEGER DEFAULT 0, switch_online INTEGER DEFAULT 0,
                controller_total INTEGER DEFAULT 0, controller_online INTEGER DEFAULT 0,
                pdu_total INTEGER DEFAULT 0, pdu_online INTEGER DEFAULT 0,
                clients_total INTEGER DEFAULT 0, clients_wireless INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_ns ON network_snapshots(network_uuid, ts);
            CREATE INDEX IF NOT EXISTS idx_ns_slug ON network_snapshots(slug, ts);

            CREATE TABLE IF NOT EXISTS uplink_quality (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                network_uuid TEXT NOT NULL,
                iface_uuid TEXT NOT NULL,
                value REAL NOT NULL,
                UNIQUE(network_uuid, iface_uuid, ts)
            );
            CREATE INDEX IF NOT EXISTS idx_uq ON uplink_quality(network_uuid, ts);

            CREATE TABLE IF NOT EXISTS location_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                area TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                UNIQUE(ts, area)
            );
            CREATE INDEX IF NOT EXISTS idx_ls ON location_snapshots(ts);

            CREATE TABLE IF NOT EXISTS event_log_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                network_uuid TEXT NOT NULL,
                network_label TEXT NOT NULL,
                slug TEXT NOT NULL,
                event_type TEXT,
                event_type_api_name TEXT,
                generated_at TEXT,
                UNIQUE(network_uuid, generated_at, event_type_api_name)
            );
            CREATE INDEX IF NOT EXISTS idx_el ON event_log_history(network_uuid, generated_at);
        """)
    print(f"DB ready: {DB_PATH}", flush=True)


def db_write_snapshot(slug: str, data: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    rows = []
    for net in data.get("networks") or []:
        nid = net["UUID"]
        devs = data.get("virtualDevices", {}).get(nid) or []
        clients = data.get("networkClients", {}).get(nid) or []
        tc: dict = {}
        oc: dict = {}
        for d in devs:
            t = d.get("deviceType", "OTHER")
            tc[t] = tc.get(t, 0) + 1
            if d.get("isOnline"):
                oc[t] = oc.get(t, 0) + 1
        rows.append((
            ts, slug, nid, net.get("label", nid),
            len(devs), sum(1 for d in devs if d.get("isOnline")),
            tc.get("ACCESS_POINT", 0), oc.get("ACCESS_POINT", 0),
            tc.get("SWITCH", 0), oc.get("SWITCH", 0),
            tc.get("CONTROLLER", 0), oc.get("CONTROLLER", 0),
            tc.get("POWER_DISTRIBUTION_UNIT", 0), oc.get("POWER_DISTRIBUTION_UNIT", 0),
            len(clients), sum(1 for c in clients if c.get("isWireless")),
        ))
    if not rows:
        return
    try:
        with _db() as conn:
            conn.executemany("""INSERT INTO network_snapshots
                (ts,slug,network_uuid,network_label,devices_total,devices_online,
                 ap_total,ap_online,switch_total,switch_online,controller_total,controller_online,
                 pdu_total,pdu_online,clients_total,clients_wireless)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    except Exception as exc:
        print(f"  db_write_snapshot error: {exc}", flush=True)


def db_write_uplink(data: dict) -> None:
    rows = []
    for nid, entry in (data.get("uplinkQuality") or {}).items():
        for v in (entry.get("values") or []):
            ts = v.get("timestamp")
            iface = v.get("phyInterfaceUUID")
            val = v.get("value")
            if ts and iface and val is not None:
                rows.append((ts, nid, iface, float(val)))
    if not rows:
        return
    try:
        with _db() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO uplink_quality(ts,network_uuid,iface_uuid,value) VALUES(?,?,?,?)",
                rows)
    except Exception as exc:
        print(f"  db_write_uplink error: {exc}", flush=True)


def db_write_events(slug: str, data: dict) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    net_map = {n["UUID"]: n.get("label", n["UUID"]) for n in (data.get("networks") or [])}
    rows = []
    for nid, entry in (data.get("eventLog") or {}).items():
        label = net_map.get(nid, nid)
        for e in (entry.get("events") or []):
            rows.append((ts, nid, label, slug,
                         e.get("eventType") or "", e.get("eventTypeAPIName") or "",
                         e.get("generatedAt") or ""))
    if not rows:
        return
    try:
        with _db() as conn:
            conn.executemany("""INSERT OR IGNORE INTO event_log_history
                (ts,network_uuid,network_label,slug,event_type,event_type_api_name,generated_at)
                VALUES(?,?,?,?,?,?,?)""", rows)
    except Exception as exc:
        print(f"  db_write_events error: {exc}", flush=True)


def db_write_location(ts: str, area_counts: dict) -> None:
    rows = [(ts, area, cnt) for area, cnt in area_counts.items()]
    if not rows:
        return
    try:
        with _db() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO location_snapshots(ts,area,count) VALUES(?,?,?)", rows)
    except Exception as exc:
        print(f"  db_write_location error: {exc}", flush=True)


def db_cleanup() -> None:
    since = (datetime.now(timezone.utc) - timedelta(days=_DB_RETENTION_DAYS)).isoformat()
    try:
        with _db() as conn:
            for tbl in ("network_snapshots", "uplink_quality", "location_snapshots", "event_log_history"):
                conn.execute(f"DELETE FROM {tbl} WHERE ts < ?", (since,))
        print(f"DB cleanup: pruned records older than {_DB_RETENTION_DAYS}d", flush=True)
    except Exception as exc:
        print(f"  db_cleanup error: {exc}", flush=True)


# ── Server-side geocoding ──────────────────────────────────────────────────────

_geocode_lock = threading.Lock()


def _server_geocode(address: str) -> tuple | None:
    """Return (lat, lng) for address, using DB cache; calls Nominatim on miss."""
    if not address:
        return None
    try:
        with _db() as conn:
            row = conn.execute("SELECT lat, lng FROM geocode_cache WHERE address=?",
                               (address,)).fetchone()
            if row is not None:
                return (row["lat"], row["lng"]) if row["lat"] is not None else None
    except Exception:
        pass
    with _geocode_lock:
        # Re-check after acquiring lock (another thread may have filled it)
        try:
            with _db() as conn:
                row = conn.execute("SELECT lat, lng FROM geocode_cache WHERE address=?",
                                   (address,)).fetchone()
                if row is not None:
                    return (row["lat"], row["lng"]) if row["lat"] is not None else None
        except Exception:
            pass
        lat = lng = None
        try:
            time.sleep(1.1)
            resp = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"format": "json", "limit": 1, "q": address},
                headers={"Accept-Language": "en", "User-Agent": "MeterDashboard/1.0"},
                timeout=10,
            )
            data = resp.json()
            if data:
                lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            pass
        try:
            ts = datetime.now(timezone.utc).isoformat()
            with _db() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO geocode_cache(address,lat,lng,updated_at) VALUES(?,?,?,?)",
                    (address, lat, lng, ts))
        except Exception:
            pass
        return (lat, lng) if lat is not None else None


def geocode_all_networks() -> None:
    """Background: geocode every network address that isn't already cached."""
    data, _ = get_snapshot()
    for slug, cdata in data.items():
        for net in (cdata.get("networks") or []):
            m = net.get("mailingAddress")
            if not m:
                continue
            addr = ", ".join(filter(None, [m.get("line1"), m.get("city"), m.get("postalCode")]))
            if addr:
                _server_geocode(addr)


@app.route("/api/geocode")
def api_geocode():
    """Return DB-cached lat/lng keyed by network UUID for all current networks."""
    data, _ = get_snapshot()
    net_addrs: dict = {}
    for slug, cdata in data.items():
        for net in (cdata.get("networks") or []):
            m = net.get("mailingAddress")
            if not m:
                continue
            addr = ", ".join(filter(None, [m.get("line1"), m.get("city"), m.get("postalCode")]))
            if addr:
                net_addrs[net["UUID"]] = addr
    cached: dict = {}
    try:
        with _db() as conn:
            for addr in set(net_addrs.values()):
                row = conn.execute("SELECT lat, lng FROM geocode_cache WHERE address=?",
                                   (addr,)).fetchone()
                if row and row["lat"] is not None:
                    cached[addr] = [row["lat"], row["lng"]]
    except Exception:
        pass
    nets = {nid: cached.get(addr) for nid, addr in net_addrs.items()}
    return jsonify({"nets": nets,
                    "total": len(net_addrs),
                    "cached": sum(1 for v in nets.values() if v)})


# ── History API routes ─────────────────────────────────────────────────────────

@app.route("/api/history/network")
def api_history_network():
    uuid = flask_request.args.get("uuid", "")
    days = min(int(flask_request.args.get("days", 7)), 90)
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _db() as conn:
        rows = conn.execute("""
            SELECT ts, devices_total, devices_online, ap_total, ap_online,
                   switch_total, switch_online, controller_total, controller_online,
                   pdu_total, pdu_online, clients_total, clients_wireless
            FROM network_snapshots WHERE network_uuid=? AND ts>=? ORDER BY ts
        """, (uuid, since)).fetchall()
    return jsonify({"rows": [dict(r) for r in rows]})


@app.route("/api/history/uplink")
def api_history_uplink():
    uuid = flask_request.args.get("uuid", "")
    days = min(int(flask_request.args.get("days", 7)), 90)
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _db() as conn:
        rows = conn.execute("""
            SELECT strftime('%Y-%m-%dT%H:%M:00Z', ts) AS ts_bucket,
                   iface_uuid, ROUND(AVG(value),4) AS value
            FROM uplink_quality WHERE network_uuid=? AND ts>=?
            GROUP BY ts_bucket, iface_uuid ORDER BY ts_bucket
        """, (uuid, since)).fetchall()
    return jsonify({"rows": [dict(r) for r in rows]})


@app.route("/api/history/events")
def api_history_events():
    uuid = flask_request.args.get("uuid", "")
    days = min(int(flask_request.args.get("days", 7)), 90)
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _db() as conn:
        if uuid:
            rows = conn.execute("""
                SELECT network_uuid, network_label, slug, event_type, event_type_api_name, generated_at
                FROM event_log_history WHERE network_uuid=? AND generated_at>=?
                ORDER BY generated_at DESC LIMIT 1000
            """, (uuid, since)).fetchall()
        else:
            rows = conn.execute("""
                SELECT network_uuid, network_label, slug, event_type, event_type_api_name, generated_at
                FROM event_log_history WHERE generated_at>=?
                ORDER BY generated_at DESC LIMIT 1000
            """, (since,)).fetchall()
    return jsonify({"events": [dict(r) for r in rows]})


# ── HTML template (filled below) ───────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Meter — Multi-Company Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin="">
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" crossorigin="">
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" crossorigin="">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg-base:#0f1117;
  --bg-sidebar:#13151f;
  --bg-section:#1a1c27;
  --bg-card:#1e2030;
  --bg-card-hover:#252840;
  --bg-active:#2d3480;
  --bg-active-light:rgba(110,128,248,.13);
  --border:#232538;
  --border2:#2a2d42;
  --text-primary:#e8e9f2;
  --text-secondary:#8b8fa8;
  --text-muted:#555870;
  --green:#3ecf6e;  --red:#f05252;
  --yellow:#f59e0b;
  --blue:#6e80f8;
  --purple:#a78bfa;
  --orange:#fb923c;
  --co0:#6e80f8;
  --co1:#a78bfa;
  --co2:#3ecf6e;
  --radius:6px;
  --sidebar-w:192px;
  --topbar-h:44px;
}
body{font-family:'Suisse Int\'l',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:var(--bg-base);color:var(--text-primary);display:flex;
  height:100vh;overflow:hidden;font-size:13px;line-height:1.5}

/* ── Sidebar ── */
.sidebar{width:var(--sidebar-w);min-width:var(--sidebar-w);background:var(--bg-sidebar);
  display:flex;flex-direction:column;border-right:1px solid var(--border);
  overflow-y:auto;flex-shrink:0}
.sidebar::-webkit-scrollbar{width:3px}
.sidebar::-webkit-scrollbar-thumb{background:var(--border2)}
.sb-top{display:flex;align-items:center;padding:12px 14px;
  border-bottom:1px solid var(--border);flex-shrink:0;gap:8px}
.sb-logo{font-weight:700;font-size:13px;color:var(--text-primary);
  display:flex;align-items:center;gap:7px;letter-spacing:-.2px}
.logo-mark{width:24px;height:24px;background:var(--blue);border-radius:7px;
  display:flex;align-items:center;justify-content:center;
  font-weight:800;font-size:12px;color:#fff;flex-shrink:0}
.sb-grp{padding:10px 0 4px}
.sb-grp-label{font-size:10px;font-weight:600;text-transform:uppercase;
  letter-spacing:.07em;color:var(--text-muted);padding:4px 14px 3px}
.nav-item{display:flex;align-items:center;gap:8px;padding:6px 14px;
  cursor:pointer;color:var(--text-secondary);font-size:12px;
  transition:background .1s,color .1s;user-select:none;border-radius:0}
.nav-item:hover{background:var(--bg-section);color:var(--text-primary)}
.nav-item.active{background:var(--bg-active);color:#fff}
.nav-icon{width:16px;text-align:center;font-size:11px;flex-shrink:0}

/* ── Main ── */
.main{flex:1;display:flex;flex-direction:column;overflow:hidden}
.topbar{display:flex;align-items:center;padding:0 18px;height:var(--topbar-h);
  border-bottom:1px solid var(--border);background:var(--bg-sidebar);gap:8px;flex-shrink:0}
.topbar-title{font-size:12px;font-weight:600;color:var(--text-primary);letter-spacing:-.1px}
.topbar-right{margin-left:auto;display:flex;align-items:center;gap:8px}
.pulse-wrap{display:flex;align-items:center;gap:5px;padding:3px 9px;
  background:var(--bg-section);border:1px solid var(--border);border-radius:5px;
  font-size:11px;color:var(--text-secondary)}
.pulse{width:7px;height:7px;border-radius:50%;background:var(--green);
  animation:pulse 2.2s ease-in-out infinite;flex-shrink:0}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
.content-area{flex:1;overflow-y:auto;overflow-x:hidden;background:var(--bg-base)}
.content-area::-webkit-scrollbar{width:5px}
.content-area::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
.section{display:none}
.section.active{display:block}
#map-section.active{display:flex;flex-direction:column;height:100%;overflow:hidden}
.sec-inner{padding:20px 24px}
.sec-hdr{margin-bottom:16px}
.sec-hdr h2{font-size:15px;font-weight:600}
.sec-hdr p{font-size:12px;color:var(--text-secondary);margin-top:3px}

/* ── Toolbar ── */
.toolbar{display:flex;align-items:center;gap:8px;margin-bottom:14px;flex-wrap:wrap}
.toolbar select,.toolbar input[type=text]{
  background:var(--bg-section);border:1px solid var(--border2);border-radius:var(--radius);
  color:var(--text-primary);padding:5px 10px;font-size:12px;font-family:inherit;outline:none}
.toolbar select:focus,.toolbar input[type=text]:focus{border-color:var(--blue)}
.toolbar input[type=text]{flex:1;min-width:150px}
.toolbar select{min-width:140px}
.btn{display:flex;align-items:center;gap:5px;background:var(--bg-card);
  border:1px solid var(--border2);border-radius:var(--radius);
  color:var(--text-secondary);padding:5px 12px;cursor:pointer;
  font-size:12px;font-family:inherit;white-space:nowrap;transition:color .1s,background .1s}
.btn:hover{background:var(--bg-card-hover);color:var(--text-primary)}
.btn.spinning .ri{display:inline-block;animation:spin .7s linear infinite}
.btn-primary{background:rgba(110,128,248,.12);border-color:rgba(110,128,248,.3);color:var(--blue)}
.btn-primary:hover{background:var(--blue);color:#fff;border-color:var(--blue)}

/* ── Stats ── */
.stats{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.stat{flex:1;min-width:100px;background:var(--bg-card);border:1px solid var(--border);
  border-radius:8px;padding:12px 16px}
.stat-lbl{font-size:11px;color:var(--text-secondary);margin-bottom:5px}
.stat-val{font-size:24px;font-weight:700;line-height:1;letter-spacing:-.5px}
.stat-sub{font-size:11px;color:var(--text-secondary);margin-top:4px}

/* ── Table ── */
.tbl-wrap{background:var(--bg-card);border:1px solid var(--border);
  border-radius:8px;overflow:hidden;margin-bottom:20px}
table{width:100%;border-collapse:collapse}
thead tr{background:var(--bg-section)}
th{padding:8px 14px;font-size:10.5px;font-weight:600;text-transform:uppercase;
  letter-spacing:.05em;color:var(--text-secondary);text-align:left;
  border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:8px 14px;border-bottom:1px solid var(--border);vertical-align:middle;
  background:var(--bg-card);font-size:12px}
tr:last-child td{border-bottom:none}
tbody tr:hover td{background:var(--bg-card-hover)}
.th-sort{cursor:pointer;user-select:none}
.th-sort:hover{color:var(--text-primary)}
.th-inner{display:flex;align-items:center;gap:4px;white-space:nowrap}
.sort-ico{font-size:9px;color:var(--blue)}
.sort-ico.dim{color:var(--text-muted)}
.cf-btn{margin-left:auto;font-size:11px;color:var(--text-muted);
  padding:1px 4px;border-radius:3px;cursor:pointer;line-height:1.2;transition:color .12s,background .12s}
.cf-btn:hover{color:var(--text-primary);background:var(--bg-section)}
.cf-btn.cf-active{color:var(--blue)}

/* ── Badges ── */
.badge{display:inline-flex;align-items:center;gap:3px;padding:2px 8px;
  border-radius:20px;font-size:10.5px;font-weight:500;white-space:nowrap}
.badge.green{background:rgba(62,207,110,.12);color:var(--green)}
.badge.red{background:rgba(240,82,82,.12);color:var(--red)}
.badge.yellow{background:rgba(245,158,11,.12);color:var(--yellow)}
.badge.blue{background:rgba(110,128,248,.12);color:var(--blue)}
.badge.purple{background:rgba(167,139,250,.12);color:var(--purple)}
.badge.orange{background:rgba(251,146,60,.12);color:var(--orange)}
.badge.dim{background:var(--bg-section);color:var(--text-secondary);border:1px solid var(--border2)}
.dot{display:inline-block;width:6px;height:6px;border-radius:50%;flex-shrink:0}
.dot.green{background:var(--green)}.dot.red{background:var(--red)}
.dot.yellow{background:var(--yellow)}.dot.gray{background:var(--text-muted)}

/* ── Misc ── */
.mono{font-family:'SF Mono',ui-monospace,monospace;font-size:11px}
.empty,.loading{padding:52px;text-align:center;color:var(--text-secondary)}
.empty .title,.loading .title{font-size:14px;font-weight:500;color:var(--text-muted);margin-bottom:6px}
.spinner{width:18px;height:18px;border:2px solid var(--border2);border-top-color:var(--blue);
  border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 12px}
@keyframes spin{to{transform:rotate(360deg)}}
.sig{display:inline-flex;align-items:flex-end;gap:2px;height:14px}
.sig-bar{width:3px;border-radius:1px}
.tbar-wrap{display:flex;align-items:center;gap:8px;min-width:130px}
.tbar-bg{flex:1;height:3px;background:var(--border);border-radius:2px;overflow:hidden}
.tbar-fill{height:100%;border-radius:2px;background:var(--green);transition:width .5s ease}
.tbar-fill.up{background:var(--blue)}
.err-banner{background:rgba(240,82,82,.1);border:1px solid rgba(240,82,82,.25);
  border-radius:var(--radius);padding:10px 14px;margin-bottom:14px;font-size:12px;color:var(--red)}
.cf-chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.cf-chip{display:inline-flex;align-items:center;gap:4px;
  background:rgba(110,128,248,.12);border:1px solid rgba(110,128,248,.25);
  border-radius:20px;padding:3px 8px;font-size:11px;color:var(--blue)}
.cf-chip-col{font-weight:600}.cf-chip-mode{color:var(--text-secondary)}
.cf-chip-val{max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cf-chip-x{background:none;border:none;color:var(--text-secondary);cursor:pointer;
  font-size:10px;padding:0;margin-left:2px;line-height:1}
.cf-chip-x:hover{color:var(--text-primary)}
#col-filter-pop{display:none;position:fixed;z-index:600;width:230px;
  background:var(--bg-sidebar);border:1px solid var(--border2);border-radius:var(--radius);
  box-shadow:0 8px 24px rgba(0,0,0,.5);font-size:12px}
.cfp-hdr{display:flex;justify-content:space-between;align-items:center;
  padding:8px 12px;border-bottom:1px solid var(--border);font-weight:600}
.cfp-x{background:none;border:none;color:var(--text-secondary);cursor:pointer;font-size:13px;padding:0}
.cfp-x:hover{color:var(--text-primary)}
.cfp-mode{display:flex;gap:14px;padding:7px 12px;border-bottom:1px solid var(--border);color:var(--text-secondary)}
.cfp-mode label{cursor:pointer;display:flex;align-items:center;gap:5px}
.cfp-search{padding:6px 8px;border-bottom:1px solid var(--border)}
.cfp-search input{width:100%;background:var(--bg-section);border:1px solid var(--border2);
  border-radius:4px;color:var(--text-primary);padding:4px 8px;font-size:11px;font-family:inherit;outline:none}
.cfp-list{max-height:180px;overflow-y:auto;padding:4px 0}
.cfv-row{display:flex;align-items:center;gap:8px;padding:5px 12px;cursor:pointer;
  color:var(--text-secondary);transition:background .1s}
.cfv-row:hover{background:var(--bg-section);color:var(--text-primary)}
.cfv-row input[type=checkbox]{cursor:pointer;accent-color:var(--blue)}
.cfp-ftr{display:flex;justify-content:flex-end;gap:6px;padding:7px 12px;border-top:1px solid var(--border)}
.cfp-btn{background:var(--bg-section);border:1px solid var(--border2);border-radius:4px;
  color:var(--text-secondary);padding:3px 10px;font-size:11px;cursor:pointer;font-family:inherit}
.cfp-btn:hover{color:var(--text-primary);background:var(--bg-card-hover)}
.cfp-done{background:rgba(110,128,248,.12);color:var(--blue);border-color:rgba(110,128,248,.3)}
.cfp-done:hover{background:var(--blue);color:#fff}

/* ── Overview ── */
.ov-controls{display:flex;align-items:center;gap:8px;margin-bottom:18px;flex-wrap:wrap}
.ov-controls h2{font-size:15px;font-weight:600;margin-right:auto}
.view-toggle{display:flex;border:1px solid var(--border2);border-radius:var(--radius);overflow:hidden}
.vtbtn{background:none;border:none;padding:5px 14px;font-size:12px;font-family:inherit;
  cursor:pointer;color:var(--text-secondary);transition:background .1s,color .1s}
.vtbtn.active{background:var(--bg-active);color:#fff}
.vtbtn:not(.active):hover{background:var(--bg-section);color:var(--text-primary)}
.co-grid{display:flex;flex-direction:column;gap:14px}
.co-card{background:var(--bg-card);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.co-card-hdr{display:flex;align-items:center;gap:10px;padding:14px 16px;
  border-bottom:1px solid var(--border)}
.co-badge{width:28px;height:28px;border-radius:8px;display:flex;align-items:center;
  justify-content:center;font-weight:800;font-size:13px;color:#fff;flex-shrink:0}
.co-name{font-weight:600;font-size:14px}
.co-pill-row{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto;align-items:center}
.co-stats-row{display:flex;gap:0;border-bottom:1px solid var(--border);flex-wrap:wrap}
.co-stat-cell{flex:1;min-width:130px;padding:10px 16px;border-right:1px solid var(--border)}
.co-stat-cell:last-child{border-right:none}
.co-stat-lbl{font-size:10.5px;color:var(--text-muted);margin-bottom:4px;text-transform:uppercase;letter-spacing:.04em}
.co-stat-chips{display:flex;gap:5px;flex-wrap:wrap}
.co-networks{padding:4px 0 6px}
.net-row{display:flex;align-items:center;gap:8px;padding:7px 16px;
  cursor:pointer;transition:background .1s;font-size:12px}
.net-row:hover{background:var(--bg-card-hover)}
.net-name{font-weight:500;color:var(--text-primary);min-width:160px}
.net-chips{display:flex;gap:5px;flex-wrap:wrap;margin-left:auto}
.net-addr{font-size:11px;color:var(--text-muted);margin-left:8px}

/* ── Map (standalone tab) ── */
.map-topbar{display:flex;align-items:center;gap:10px;padding:10px 18px;
  border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap;background:var(--bg-sidebar)}
.map-legend{display:flex;align-items:center;gap:14px;margin-left:auto;
  font-size:11px;color:var(--text-secondary);flex-wrap:wrap}
.map-legend-item{display:flex;align-items:center;gap:5px}
.map-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.map-wrap{flex:1;min-height:400px;overflow:hidden}
.ov-map-wrap{height:calc(100vh - 150px);min-height:420px;border-radius:8px;overflow:hidden;
  border:1px solid var(--border);margin-top:4px}

/* ── Leaflet popup dark theme ── */
.meter-popup .leaflet-popup-content-wrapper{
  background:#1e2030;border:1px solid #2a2d42;
  color:#e8e9f2;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,.5);padding:0}
.meter-popup .leaflet-popup-content{margin:12px 14px}
.meter-popup .leaflet-popup-tip-container{display:none}

/* ── Light mode ── */
body.light{
  --bg-base:#f3f4f8;
  --bg-sidebar:#ffffff;
  --bg-section:#eef0f6;
  --bg-card:#ffffff;
  --bg-card-hover:#e8eaf2;
  --bg-active:#dde2ff;
  --bg-active-light:rgba(110,128,248,.1);
  --border:#dde0ec;
  --border2:#cdd0e0;
  --text-primary:#1a1c2e;
  --text-secondary:#555870;
  --text-muted:#9099b8;
}
body.light .meter-popup .leaflet-popup-content-wrapper{
  background:#ffffff;border-color:#dde0ec;color:#1a1c2e}
.theme-toggle{display:flex;align-items:center;justify-content:center;
  width:30px;height:30px;border-radius:6px;cursor:pointer;
  background:var(--bg-section);border:1px solid var(--border2);
  color:var(--text-secondary);font-size:15px;
  transition:background .15s,color .15s}
.theme-toggle:hover{background:var(--bg-card-hover);color:var(--text-primary)}

/* ── Overview device columns ── */
.dev-cols{display:grid;grid-template-columns:repeat(4,1fr);gap:0;border-bottom:1px solid var(--border)}
.dev-col{padding:10px 14px;border-right:1px solid var(--border)}
.dev-col:last-child{border-right:none}
.dev-col-lbl{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin-bottom:6px}
.dev-col-name{font-size:11px;font-weight:600;color:var(--text-secondary);margin-bottom:5px}
.dev-counter-row{display:flex;gap:5px;flex-wrap:wrap}
.dev-ctr{display:flex;align-items:center;gap:3px;font-size:11px;font-weight:600;padding:2px 6px;border-radius:4px}
.dev-ctr.online{background:rgba(62,207,110,.12);color:#3ecf6e}
.dev-ctr.alerting{background:rgba(245,158,11,.12);color:#f59e0b}
.dev-ctr.offline{background:rgba(240,82,82,.12);color:#f05252}
.dev-ctr.dim{background:var(--bg-section);color:var(--text-muted)}
.dev-ctr-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.dev-ctr-dot.g{background:#3ecf6e}.dev-ctr-dot.y{background:#f59e0b}.dev-ctr-dot.r{background:#f05252}

/* ── Collapsible network rows ── */
.net-toggle{background:none;border:none;color:var(--text-muted);cursor:pointer;
  font-size:11px;padding:2px 4px;margin-left:auto;line-height:1;flex-shrink:0}
.net-rows-wrap{overflow:hidden;transition:max-height .2s ease}
.net-rows-wrap.collapsed{max-height:0!important}

/* ── Config Drift ── */
.drift-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
.drift-card{background:var(--bg-card);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.drift-card-hdr{padding:10px 14px;border-bottom:1px solid var(--border);font-weight:600;font-size:12px;
  display:flex;align-items:center;gap:8px}
.drift-item{padding:7px 14px;border-bottom:1px solid var(--border);font-size:12px;display:flex;gap:8px;align-items:center;cursor:pointer;user-select:none}
.drift-item:hover{background:rgba(255,255,255,.02)}
.drift-row-wrap:last-child .drift-item{border-bottom:none}
.drift-toggle{font-size:9px;color:var(--text-muted);transition:transform .15s;display:inline-block;margin-right:2px;flex-shrink:0}
.drift-row-wrap.open .drift-toggle{transform:rotate(90deg)}
.drift-detail{display:none;padding:10px 14px 12px;background:var(--bg-section);border-bottom:1px solid var(--border)}
.drift-row-wrap.open .drift-detail{display:block}
.drift-cmp-tbl{width:100%;border-collapse:collapse;font-size:11px}
.drift-cmp-tbl td{padding:4px 8px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:top}
.drift-cmp-tbl tr:last-child td{border-bottom:none}
.drift-cmp-tbl td:first-child{color:var(--text-muted);width:90px;font-size:10px;white-space:nowrap}
.drift-cmp-tbl td.val-diff{color:var(--yellow)}
.drift-match{color:var(--green);font-size:11px}
.drift-miss{color:var(--red);font-size:11px}
.drift-warn{color:var(--yellow);font-size:11px}
.drift-controls{display:flex;gap:10px;align-items:center;margin-bottom:18px;flex-wrap:wrap}
.drift-select{background:var(--bg-section);border:1px solid var(--border);border-radius:var(--radius);
  color:var(--text-primary);padding:6px 10px;font-size:12px;font-family:inherit;outline:none}
.drift-summary-row{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.drift-summ{flex:1;min-width:100px;background:var(--bg-card);border:1px solid var(--border);
  border-radius:var(--radius);padding:10px 14px;text-align:center}
.drift-summ-num{font-size:22px;font-weight:700;line-height:1}
.drift-summ-lbl{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin-top:3px}

/* ── PCI / Location embedded ── */
.embedded-frame{background:var(--bg-card);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.embedded-frame .sec-inner{padding:20px 24px}
.wizard-step{display:none}.wizard-step.active{display:block}
.pci-net-card{display:flex;align-items:center;gap:12px;background:var(--bg-section);
  border:1px solid var(--border);border-radius:var(--radius);padding:11px 14px;
  cursor:pointer;margin-bottom:6px;transition:border-color .1s}
.pci-net-card:hover{border-color:var(--blue)}
.pci-net-card.sel{border-color:var(--blue);background:rgba(110,128,248,.08)}
.pci-finding{background:var(--bg-section);border:1px solid var(--border);border-radius:var(--radius);
  margin-bottom:10px;overflow:hidden}
.pci-finding.f-FAIL{border-left:3px solid var(--red)}
.pci-finding.f-WARNING{border-left:3px solid var(--yellow)}
.pci-finding.f-PASS{border-left:3px solid var(--green)}
.pci-finding.f-MANUAL{border-left:3px solid var(--blue)}
.pci-finding.f-INFO{border-left:3px solid var(--text-muted)}
.pci-fhdr{display:flex;align-items:center;gap:8px;padding:9px 12px;cursor:pointer}
.pci-fhdr:hover{background:var(--bg-card-hover)}
.pci-fbody{display:none;padding:0 12px 10px;border-top:1px solid var(--border)}
.pci-fbody.open{display:block}
.pci-det-list{list-style:none;margin:8px 0}
.pci-det-list li{font-size:11px;font-family:monospace;color:var(--text-secondary);
  padding:2px 0;border-bottom:1px solid var(--border)}
.pci-det-list li:last-child{border-bottom:none}
.pci-cde-cols{display:grid;grid-template-columns:1fr 1fr;gap:0}
.pci-cde-col{padding:12px}
.pci-cde-col+.pci-cde-col{border-left:1px solid var(--border)}
.pci-cde-lbl{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin-bottom:8px}
.pci-item{display:flex;align-items:flex-start;gap:8px;padding:5px 0;cursor:pointer;font-size:12px}
.loc-chart-card{background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:14px 16px;margin-bottom:16px}
.loc-chart-wrap{position:relative;height:220px}

/* ── CSV download button ── */
.csv-btn{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;
  background:var(--bg-section);border:1px solid var(--border);border-radius:4px;
  font-size:11px;color:var(--text-secondary);cursor:pointer;margin-left:auto;white-space:nowrap}
.csv-btn:hover{background:var(--bg-card);color:var(--text-primary)}
.tbl-actions{display:flex;align-items:center;margin-bottom:6px}

/* ── History ── */
.hist-controls{display:flex;gap:10px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
.hist-co-sel,.hist-net-sel{background:var(--bg-section);border:1px solid var(--border);
  border-radius:var(--radius);color:var(--text-primary);padding:6px 10px;
  font-size:12px;font-family:inherit;outline:none;min-width:160px}
.hist-range-btns{display:flex;gap:4px}
.hist-range{background:var(--bg-section);border:1px solid var(--border);border-radius:var(--radius);
  color:var(--text-muted);padding:4px 10px;font-size:11px;cursor:pointer;font-family:inherit}
.hist-range.active{background:var(--blue);border-color:var(--blue);color:#fff}
.hist-chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
.hist-chart-card{background:var(--bg-card);border:1px solid var(--border);border-radius:8px;
  padding:16px 18px}
.hist-chart-title{font-size:11px;color:var(--text-muted);font-weight:500;
  text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
.hist-chart-wrap{position:relative;height:180px}
.hist-chart-full{grid-column:1/-1}
.hist-event-tbl{width:100%;border-collapse:collapse;font-size:12px}
.hist-event-tbl th{background:var(--bg-section);padding:6px 10px;
  text-align:left;font-weight:500;color:var(--text-muted);font-size:10px;
  text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}
.hist-event-tbl td{padding:6px 10px;border-bottom:1px solid var(--border);
  color:var(--text-secondary)}
.hist-event-tbl tr:last-child td{border-bottom:none}
.hist-db-info{font-size:11px;color:var(--text-muted);margin-bottom:14px}
</style>
</head>
<body>

<!-- ── Sidebar ── -->
<aside class="sidebar">
  <div class="sb-top">
    <div class="sb-logo"><div class="logo-mark">m</div>meter</div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">Overview</div>
    <div class="nav-item active" data-tab="overview">
      <span class="nav-icon">◉</span>Overview
    </div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">ISP</div>
    <div class="nav-item" data-tab="uplink-quality">
      <span class="nav-icon">▲</span>Uplink Quality
    </div>
    <div class="nav-item" data-tab="throughput">
      <span class="nav-icon">⇅</span>Throughput
    </div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">Network</div>
    <div class="nav-item" data-tab="clients">
      <span class="nav-icon">◎</span>Clients
    </div>
    <div class="nav-item" data-tab="ssids">
      <span class="nav-icon">▸</span>SSIDs
    </div>
    <div class="nav-item" data-tab="vlans">
      <span class="nav-icon">⊟</span>VLANs
    </div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">Infrastructure</div>
    <div class="nav-item" data-tab="devices">
      <span class="nav-icon">⬡</span>Devices
    </div>
    <div class="nav-item" data-tab="switch-ports">
      <span class="nav-icon">▦</span>Switch Ports
    </div>
    <div class="nav-item" data-tab="events">
      <span class="nav-icon">≡</span>Event Log
    </div>
    <div class="nav-item" data-tab="net-history">
      <span class="nav-icon">↗</span>History
    </div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">Views</div>
    <div class="nav-item" data-tab="map-section">
      <span class="nav-icon">◈</span>Map
    </div>
  </div>

  <div class="sb-grp">
    <div class="sb-grp-label">Reports</div>
    <div class="nav-item" data-tab="config-drift">
      <span class="nav-icon">⇄</span>Config Drift
    </div>
    <div class="nav-item" data-tab="pci-report">
      <span class="nav-icon">✔</span>PCI Compliance
    </div>
    <div class="nav-item" data-tab="location-analytics">
      <span class="nav-icon">◐</span>Location Analytics
    </div>
  </div>
</aside>

<!-- ── Main ── -->
<div class="main">
  <div class="topbar">
    <span class="topbar-title">Multi-Company Dashboard</span>
    <div class="topbar-right">
      <span id="co-count" style="font-size:11px;color:var(--text-muted)"></span>
      <button class="theme-toggle" id="theme-toggle-btn" onclick="toggleTheme()" title="Toggle light/dark mode">🌙</button>
      <div class="pulse-wrap">
        <div class="pulse"></div>
        <span id="status-text">Loading…</span>
      </div>
    </div>
  </div>

  <div class="content-area">

    <!-- Overview -->
    <div id="overview" class="section active">
      <div class="sec-inner">
        <div class="ov-controls">
          <h2>Overview</h2>
          <select id="ov-co-filter" onchange="setOvCompany(this.value)" style="min-width:160px;background:var(--bg-section);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text-primary);padding:5px 10px;font-size:12px;font-family:inherit;outline:none">
            <option value="all">All Companies</option>
          </select>
          <div class="view-toggle">
            <button class="vtbtn active" id="ov-grid-btn" onclick="setOvView('grid')">Grid</button>
            <button class="vtbtn" id="ov-map-btn" onclick="setOvView('map')">Map</button>
          </div>
          <button class="btn" onclick="triggerRefresh()"><span class="ri">↺</span> Refresh</button>
        </div>
        <div id="ov-err"></div>
        <div id="ov-grid"></div>
        <div id="ov-map-wrap" style="display:none">
          <div id="ov-map-container" class="ov-map-wrap"></div>
        </div>
      </div>
    </div>

    <!-- Uplink Quality -->
    <div id="uplink-quality" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Uplink Quality</h2>
          <p>WAN quality score per uplink — last 4 h, 5-min buckets</p></div>
        <div id="uplink-quality-toolbar"></div>
        <div id="uplink-quality-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Throughput -->
    <div id="throughput" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Uplink Throughput</h2>
          <p>WAN bandwidth per interface — last 4 h, 5-min buckets</p></div>
        <div id="throughput-toolbar"></div>
        <div id="throughput-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Clients -->
    <div id="clients" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Clients</h2>
          <p>Active clients with AP, VLAN, SSID and switch port details</p></div>
        <div id="clients-toolbar"></div>
        <div id="clients-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Devices -->
    <div id="devices" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Devices</h2>
          <p>All virtual devices with type, model, online status and uptime</p></div>
        <div id="devices-toolbar"></div>
        <div id="devices-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- SSIDs -->
    <div id="ssids" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>SSIDs</h2>
          <p>Wireless networks by company and site</p></div>
        <div id="ssids-toolbar"></div>
        <div id="ssids-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- VLANs -->
    <div id="vlans" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>VLANs</h2>
          <p>VLAN configuration and status across all networks</p></div>
        <div id="vlans-toolbar"></div>
        <div id="vlans-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Switch Ports -->
    <div id="switch-ports" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Switch Ports</h2>
          <p>Port traffic, config and error counters per switch</p></div>
        <div id="switch-ports-toolbar"></div>
        <div id="switch-ports-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Events -->
    <div id="events" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Event Log</h2>
          <p>Most recent 50 events per network, newest first</p></div>
        <div id="events-toolbar"></div>
        <div id="events-body"><div class="loading"><div class="spinner"></div><div class="title">Fetching…</div></div></div>
      </div>
    </div>

    <!-- Map tab -->
    <div id="map-section" class="section">
      <div class="map-topbar">
        <span style="font-size:12px;font-weight:600">Network Map</span>
        <span id="map-tab-status" style="font-size:11px;color:var(--text-secondary)">Loading…</span>
        <select id="map-co-filter" onchange="updateMapTab()" style="background:var(--bg-section);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text-primary);padding:4px 8px;font-size:11px;font-family:inherit;outline:none">
          <option value="all">All Companies</option>
        </select>
        <div class="map-legend">
          <span class="map-legend-item"><span class="map-dot" style="background:#3ecf6e"></span>All Online</span>
          <span class="map-legend-item"><span class="map-dot" style="background:#f59e0b"></span>Partial</span>
          <span class="map-legend-item"><span class="map-dot" style="background:#f05252"></span>Offline</span>
        </div>
      </div>
      <div id="map-tab-container" class="map-wrap"></div>
    </div>

    <!-- Config Drift -->
    <div id="config-drift" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Configuration Drift</h2>
          <p>Compare VLAN and SSID configuration across networks to identify inconsistencies</p></div>
        <div class="drift-controls">
          <select class="drift-select" id="drift-co-a" onchange="driftRefresh()">
            <option value="">Select Company A</option></select>
          <select class="drift-select" id="drift-net-a" onchange="driftRefresh()">
            <option value="">Select Network A</option></select>
          <span style="color:var(--text-muted);font-size:13px">vs</span>
          <select class="drift-select" id="drift-co-b" onchange="driftRefresh()">
            <option value="">Select Company B</option></select>
          <select class="drift-select" id="drift-net-b" onchange="driftRefresh()">
            <option value="">Select Network B</option></select>
        </div>
        <div id="drift-summary-row" class="drift-summary-row" style="display:none"></div>
        <div id="drift-body"></div>
      </div>
    </div>

    <!-- PCI Compliance -->
    <div id="pci-report" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>PCI-DSS v4.0 Compliance Report</h2>
          <p>Assess network compliance against PCI-DSS v4.0 requirements</p></div>
        <div style="display:flex;gap:8px;margin-bottom:16px" id="pci-step-nav">
          <button class="btn" id="pci-nav-1" onclick="pciShowStep(1)" style="font-size:11px;padding:4px 12px">① Networks</button>
          <button class="btn" id="pci-nav-2" onclick="pciShowStep(2)" style="font-size:11px;padding:4px 12px;opacity:.4" disabled>② CDE</button>
          <button class="btn" id="pci-nav-3" onclick="pciShowStep(3)" style="font-size:11px;padding:4px 12px;opacity:.4" disabled>③ Report</button>
        </div>
        <div id="pci-step-1" class="wizard-step active">
          <div id="pci-net-list"><div class="loading"><div class="spinner"></div><div class="title">Loading networks…</div></div></div>
          <div style="margin-top:14px">
            <button class="btn" id="pci-btn-cde" onclick="pciGotoCDE()" disabled>Configure CDE →</button>
          </div>
        </div>
        <div id="pci-step-2" class="wizard-step">
          <div id="pci-cde-config"></div>
          <div style="margin-top:14px;display:flex;gap:8px">
            <button class="btn" onclick="pciShowStep(1)">← Back</button>
            <button class="btn" id="pci-btn-gen" onclick="pciGenerateReport()">Generate Report →</button>
          </div>
        </div>
        <div id="pci-step-3" class="wizard-step">
          <div id="pci-report-content"></div>
          <div style="margin-top:14px;display:flex;gap:8px">
            <button class="btn" onclick="pciShowStep(2)">← Back</button>
            <button class="btn" onclick="window.print()">⬇ Print / PDF</button>
          </div>
        </div>
      </div>
    </div>

    <!-- Location Analytics -->
    <div id="location-analytics" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Location Analytics</h2>
          <p>Wireless client count by building area — 5-minute snapshots from the Primary network</p></div>
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:14px">
          <button class="btn" onclick="locRefresh()">↺ Refresh</button>
          <span id="loc-status" style="font-size:11px;color:var(--text-muted)">Loading…</span>
        </div>
        <div id="loc-stats-row" class="stats" style="margin-bottom:16px"></div>
        <div id="loc-body"></div>
      </div>
    </div>

    <div id="net-history" class="section">
      <div class="sec-inner">
        <div class="sec-hdr"><h2>Network History</h2>
          <p>Device health, uplink quality and client trends stored in the local database (up to 90 days)</p>
        </div>
        <div class="hist-controls">
          <select class="hist-co-sel" id="hist-co-sel" onchange="histPopulateNets()">
            <option value="">— Company —</option>
          </select>
          <select class="hist-net-sel" id="hist-net-sel" onchange="histLoad()">
            <option value="">— Network —</option>
          </select>
          <div class="hist-range-btns">
            <button class="hist-range active" onclick="histSetRange(1)">24h</button>
            <button class="hist-range" onclick="histSetRange(7)">7d</button>
            <button class="hist-range" onclick="histSetRange(30)">30d</button>
            <button class="hist-range" onclick="histSetRange(90)">90d</button>
          </div>
        </div>
        <div id="hist-db-info" class="hist-db-info"></div>
        <div id="hist-body">
          <div class="empty-state" style="margin-top:40px">Select a network to view historical data</div>
        </div>
      </div>
    </div>

  </div><!-- .content-area -->
</div><!-- .main -->

<!-- Column filter popover -->
<div id="col-filter-pop">
  <div class="cfp-hdr"><span id="cfp-title"></span><button class="cfp-x" onclick="closeColFilter()">✕</button></div>
  <div class="cfp-mode">
    <label><input type="radio" name="cfpmode" value="include" onchange="setCFMode(this.value)"> Include</label>
    <label><input type="radio" name="cfpmode" value="exclude" onchange="setCFMode(this.value)"> Exclude</label>
  </div>
  <div class="cfp-search"><input type="text" placeholder="Filter values…" oninput="cfpSearch(this.value)"></div>
  <div class="cfp-list" id="cfp-list"></div>
  <div class="cfp-ftr">
    <button class="cfp-btn" onclick="clearCF()">Clear</button>
    <button class="cfp-btn cfp-done" onclick="closeColFilter()">Done</button>
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js" crossorigin=""></script>
<script>
// ── Constants ─────────────────────────────────────────────────────────────────
const CO_LABELS = {
  'meter':                       'Meter',
  'meters-internal-beta-dogfood':'Meters Internal',
  'meter-events':                'Meter Events',
};
const CO_COLORS = ['#6e80f8','#a78bfa','#3ecf6e','#fb923c','#f59e0b'];
let _coColorMap = {};

function coLabel(slug) { return CO_LABELS[slug] || slug.replace(/-/g,' ').replace(/\b\w/g,c=>c.toUpperCase()); }
function coColor(slug) { return _coColorMap[slug] || '#6e80f8'; }
function coInitial(slug) { return coLabel(slug)[0].toUpperCase(); }

// ── Theme ─────────────────────────────────────────────────────────────────────
(function(){
  const saved = localStorage.getItem('theme');
  if(saved==='light') document.body.classList.add('light');
})();
function toggleTheme() {
  const isLight = document.body.classList.toggle('light');
  localStorage.setItem('theme', isLight ? 'light' : 'dark');
  const btn = document.getElementById('theme-toggle-btn');
  if(btn) btn.textContent = isLight ? '☀️' : '🌙';
}
(function(){
  const btn = document.getElementById('theme-toggle-btn');
  if(btn && document.body.classList.contains('light')) btn.textContent = '☀️';
})();

// ── State ─────────────────────────────────────────────────────────────────────
let appData      = {};   // {slug → {networks, networkClients, …}}
let companySlugs = [];
let activeTab    = 'overview';
let lastUpdated  = {};
let toolbarsReady = false;
let ovView       = 'grid';
let ovCompany    = 'all';

const TABS = ['uplink-quality','throughput','clients','devices','ssids','vlans','switch-ports','events'];
const filterState = {};
TABS.forEach(t => {
  filterState[t] = { company:'all', network:'all', search:'', sortCol:null, sortDir:'asc', colFilters:{} };
});
const _colValues = {};

// ── Utilities ─────────────────────────────────────────────────────────────────
function esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function fmtBytes(b) {
  if (!b) return '0 B';
  const u=['B','KB','MB','GB','TB'],i=Math.min(Math.floor(Math.log(Math.abs(b))/Math.log(1024)),u.length-1);
  return (b/Math.pow(1024,i)).toFixed(i?1:0)+'\u202f'+u[i];
}
function fmtBps(v) {
  if (!v) return '0 bps';
  const u=['bps','Kbps','Mbps','Gbps'],i=Math.min(Math.floor(Math.log(Math.abs(v))/Math.log(1000)),u.length-1);
  return (v/Math.pow(1000,i)).toFixed(i?1:0)+'\u202f'+u[i];
}
function timeAgo(ts) {
  if (!ts) return '—';
  const s=Math.floor((Date.now()-new Date(ts))/1000);
  if(s<60) return s+'s ago';
  if(s<3600) return Math.floor(s/60)+'m ago';
  if(s<86400) return Math.floor(s/3600)+'h ago';
  return Math.floor(s/86400)+'d ago';
}
function fmtUptime(iso) {
  if (!iso) return '—';
  const m=/P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:\d+S)?/.exec(iso);
  if (!m) return iso;
  const parts=[];
  if(+(m[1]||0)) parts.push(m[1]+'d');
  if(+(m[2]||0)) parts.push(m[2]+'h');
  if(+(m[3]||0)) parts.push(m[3]+'m');
  return parts.length?parts.join(' '):'<1m';
}
function badge(l,c){return `<span class="badge ${c}">${esc(l)}</span>`;}
function dot(c){return `<span class="dot ${c}"></span>`;}
function empty(msg,sub){return `<div class="empty"><div class="title">${esc(msg)}</div>${sub?`<div>${esc(sub)}</div>`:''}</div>`;}
function signalBars(dbm) {
  if(dbm==null) return '—';
  const lvl=dbm>-55?4:dbm>-65?3:dbm>-75?2:1;
  const col=dbm>-65?'var(--green)':dbm>-75?'var(--yellow)':'var(--red)';
  const bars=[4,8,11,14].map((h,i)=>`<div class="sig-bar" style="height:${h}px;background:${i<lvl?col:'var(--border)'}"></div>`).join('');
  return `<span class="sig">${bars}</span> <span style="font-size:11px;color:var(--text-secondary)">${dbm} dBm</span>`;
}
function tBar(val,max,dir) {
  const pct=max>0?Math.min((val/max)*100,100):0;
  return `<div class="tbar-wrap"><div class="tbar-bg"><div class="tbar-fill ${dir==='upload'?'up':''}" style="width:${pct.toFixed(1)}%"></div></div>
    <span style="color:var(--text-primary);min-width:68px;text-align:right">${fmtBps(val)}</span></div>`;
}

// ── Multi-company data helpers ────────────────────────────────────────────────
function allNetworks() {
  return companySlugs.flatMap(s => (appData[s]||{}).networks||[]).map(n => ({...n, _slug: _netSlug(n.UUID)}));
}
function _netSlug(nid) {
  for (const s of companySlugs) {
    if (((appData[s]||{}).networks||[]).some(n=>n.UUID===nid)) return s;
  }
  return null;
}
function netsForCompany(coFilter) {
  if (coFilter==='all') return allNetworks();
  return ((appData[coFilter]||{}).networks||[]).map(n=>({...n,_slug:coFilter}));
}
function selectedNets(coFilter, netFilter) {
  const ns = netsForCompany(coFilter);
  return netFilter==='all' ? ns : ns.filter(n=>n.UUID===netFilter);
}
function getData(slug, key, nid) {
  return ((appData[slug]||{})[key]||{})[nid] || [];
}
function getDataObj(slug, key, nid) {
  return ((appData[slug]||{})[key]||{})[nid] || {};
}
function isMultiCo(coFilter) { return coFilter==='all' && companySlugs.length > 1; }
function isMultiNet(coFilter, netFilter) { return selectedNets(coFilter, netFilter).length > 1; }

// ── Sort / col-filter helpers (same pattern as before) ────────────────────────
function sortClick(tab, col) {
  const s=filterState[tab];
  if(s.sortCol===col) s.sortDir=s.sortDir==='asc'?'desc':'asc';
  else {s.sortCol=col;s.sortDir='asc';}
  renderTab(tab);
}
function sortIcon(tab,col) {
  const s=filterState[tab];
  if(s.sortCol!==col) return '<span class="sort-ico dim">⇅</span>';
  return `<span class="sort-ico">${s.sortDir==='asc'?'↑':'↓'}</span>`;
}
function applySort(rows,tab,getVal) {
  const {sortCol,sortDir}=filterState[tab];
  if(!sortCol) return rows;
  return rows.slice().sort((a,b)=>{
    const va=getVal(a,sortCol),vb=getVal(b,sortCol);
    if(va==null&&vb==null) return 0;
    if(va==null) return sortDir==='asc'?1:-1;
    if(vb==null) return sortDir==='asc'?-1:1;
    if(typeof va==='number'&&typeof vb==='number') return sortDir==='asc'?va-vb:vb-va;
    return sortDir==='asc'?String(va).localeCompare(String(vb),undefined,{numeric:true})
                          :String(vb).localeCompare(String(va),undefined,{numeric:true});
  });
}
function hasColFilter(tab,col){const cf=(filterState[tab].colFilters||{})[col];return !!(cf&&cf.values&&cf.values.size>0);}
function colFilterMatch(tab,col,val){
  const cf=(filterState[tab].colFilters||{})[col];
  if(!cf||!cf.values||!cf.values.size) return true;
  const has=cf.values.has(String(val??''));
  return cf.mode==='include'?has:!has;
}
function applyColFilters(rows,tab,getVal){
  const cf=filterState[tab].colFilters||{};
  const active=Object.keys(cf).filter(col=>cf[col].values&&cf[col].values.size>0);
  if(!active.length) return rows;
  return rows.filter(row=>active.every(col=>colFilterMatch(tab,col,getVal(row,col))));
}
function storeColVals(tab,col,vals){
  _colValues[tab+'_'+col]=[...new Set(vals.filter(v=>v!=null&&v!=='').map(String))].sort((a,b)=>a.localeCompare(b,undefined,{numeric:true}));
}
let _cfTab=null,_cfCol=null;
function openColFilter(e,tab,col){
  e.stopPropagation();
  if(_cfTab===tab&&_cfCol===col){closeColFilter();return;}
  _cfTab=tab;_cfCol=col;
  const cf=(filterState[tab].colFilters||{})[col]||{mode:'include',values:new Set()};
  const vals=_colValues[tab+'_'+col]||[];
  document.getElementById('cfp-title').textContent=col;
  document.querySelectorAll('#col-filter-pop input[name=cfpmode]').forEach(r=>{r.checked=r.value===(cf.mode||'include');});
  document.querySelector('.cfp-search input').value='';
  const list=document.getElementById('cfp-list');
  list.innerHTML=vals.length?vals.map(v=>{const sv=String(v),checked=cf.values&&cf.values.has(sv)?'checked':'';
    return `<label class="cfv-row"><input type="checkbox" value="${esc(sv)}" ${checked} onchange="toggleCFV(this.value,this.checked)"><span>${esc(sv)||'<em style="color:var(--text-muted)">empty</em>'}</span></label>`;
  }).join(''):'<div style="padding:10px 12px;color:var(--text-muted)">No values</div>';
  const th=e.target.closest('th');
  const rect=th?th.getBoundingClientRect():e.target.getBoundingClientRect();
  const pop=document.getElementById('col-filter-pop');
  pop.style.top=(rect.bottom+2)+'px';
  pop.style.left=Math.min(rect.left,window.innerWidth-238)+'px';
  pop.style.display='block';
}
function closeColFilter(){document.getElementById('col-filter-pop').style.display='none';_cfTab=null;_cfCol=null;}
function toggleCFV(val,checked){
  if(!_cfTab||!_cfCol) return;
  if(!filterState[_cfTab].colFilters[_cfCol]) filterState[_cfTab].colFilters[_cfCol]={mode:'include',values:new Set()};
  const cf=filterState[_cfTab].colFilters[_cfCol];
  if(checked) cf.values.add(String(val)); else cf.values.delete(String(val));
  renderTab(_cfTab);
}
function setCFMode(mode){
  if(!_cfTab||!_cfCol) return;
  if(!filterState[_cfTab].colFilters[_cfCol]) filterState[_cfTab].colFilters[_cfCol]={mode,values:new Set()};
  else filterState[_cfTab].colFilters[_cfCol].mode=mode;
  renderTab(_cfTab);
}
function clearCF(){if(!_cfTab||!_cfCol) return;delete filterState[_cfTab].colFilters[_cfCol];closeColFilter();renderTab(_cfTab);}
function cfpSearch(q){const ql=q.toLowerCase();document.querySelectorAll('#cfp-list .cfv-row').forEach(el=>{el.style.display=el.querySelector('span').textContent.toLowerCase().includes(ql)?'':'none';});}
document.addEventListener('click',e=>{const p=document.getElementById('col-filter-pop');if(p&&p.style.display!=='none'&&!p.contains(e.target))closeColFilter();});

function thEl(tab,col,label){
  const active=hasColFilter(tab,col);
  return `<th class="th-sort" onclick="sortClick('${tab}','${col}')"><div class="th-inner"><span>${label}</span>${sortIcon(tab,col)}<span class="cf-btn${active?' cf-active':''}" onclick="event.stopPropagation();openColFilter(event,'${tab}','${col}')" title="Filter">≡</span></div></th>`;
}
function renderColFilterChips(tab){
  const cf=filterState[tab].colFilters||{};
  const active=Object.entries(cf).filter(([,v])=>v.values&&v.values.size>0);
  if(!active.length) return '';
  const chips=active.map(([col,v])=>{const vals=[...v.values].join(', ');const ml=v.mode==='exclude'?'≠':'=';
    return `<span class="cf-chip"><span class="cf-chip-col">${esc(col)}</span> <span class="cf-chip-mode">${ml}</span> <span class="cf-chip-val" title="${esc(vals)}">${esc(vals)}</span><button class="cf-chip-x" onclick="clearCFByKey('${tab}','${col}')">✕</button></span>`;
  }).join('');
  return `<div class="cf-chips">${chips}</div>`;
}
function clearCFByKey(tab,col){delete filterState[tab].colFilters[col];renderTab(tab);}
function hits(cells,q){if(!q) return true;const ql=q.toLowerCase();return cells.some(c=>c!=null&&String(c).toLowerCase().includes(ql));}
function updateAC(tab,cells){
  const dl=document.getElementById(tab+'-ac');if(!dl) return;
  const uniq=[...new Set(cells.filter(Boolean).map(String).map(s=>s.trim()).filter(s=>s.length>1))].sort().slice(0,200);
  dl.innerHTML=uniq.map(v=>`<option value="${esc(v)}">`).join('');
}

// ── Navigation ────────────────────────────────────────────────────────────────
document.querySelectorAll('.nav-item[data-tab]').forEach(el=>{
  el.addEventListener('click',()=>{
    document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
    document.querySelectorAll('.section').forEach(s=>s.classList.remove('active'));
    el.classList.add('active');
    activeTab=el.dataset.tab;
    document.getElementById(activeTab).classList.add('active');
    renderTab(activeTab);
  });
});

// ── Toolbar ───────────────────────────────────────────────────────────────────
function buildCoOptions(tab) {
  const cur=filterState[tab].company;
  return `<option value="all"${cur==='all'?' selected':''}>All Companies</option>`+
    companySlugs.map(s=>`<option value="${s}"${cur===s?' selected':''}>${esc(coLabel(s))}</option>`).join('');
}
function buildNetOptions(tab) {
  const {company,network}=filterState[tab];
  const nets=netsForCompany(company);
  return `<option value="all"${network==='all'?' selected':''}>All Networks</option>`+
    nets.map(n=>`<option value="${n.UUID}"${network===n.UUID?' selected':''}>${esc(n.label)}</option>`).join('');
}

function initToolbars() {
  if (!toolbarsReady) {
    TABS.forEach(t=>{
      const el=document.getElementById(t+'-toolbar');
      if (!el) return;
      el.innerHTML=`<div class="toolbar">
        <select id="${t}-co-sel" onchange="filterCo('${t}',this.value)">${buildCoOptions(t)}</select>
        <select id="${t}-net-sel" onchange="filterNet('${t}',this.value)">${buildNetOptions(t)}</select>
        <input id="${t}-search-inp" type="text" placeholder="Search…" list="${t}-ac"
               value="${esc(filterState[t].search)}" oninput="filterSearch('${t}',this.value)">
        <datalist id="${t}-ac"></datalist>
        <button class="btn" id="${t}-refresh-btn" onclick="triggerRefresh()">
          <span class="ri">↺</span> Refresh
        </button>
      </div>`;
    });
    toolbarsReady=true;
  } else {
    TABS.forEach(t=>{
      const co=document.getElementById(t+'-co-sel');
      const net=document.getElementById(t+'-net-sel');
      if(co) co.innerHTML=buildCoOptions(t);
      if(net) net.innerHTML=buildNetOptions(t);
    });
    // update overview company filters
    const ovSel=document.getElementById('ov-co-filter');
    if(ovSel) ovSel.innerHTML=`<option value="all"${ovCompany==='all'?' selected':''}>All Companies</option>`+
      companySlugs.map(s=>`<option value="${s}"${ovCompany===s?' selected':''}>${esc(coLabel(s))}</option>`).join('');
    const mapSel=document.getElementById('map-co-filter');
    if(mapSel) mapSel.innerHTML=`<option value="all">All Companies</option>`+
      companySlugs.map(s=>`<option value="${s}">${esc(coLabel(s))}</option>`).join('');
  }
}

function filterCo(tab,val){
  filterState[tab].company=val;
  filterState[tab].network='all';
  const net=document.getElementById(tab+'-net-sel');
  if(net) net.innerHTML=buildNetOptions(tab);
  renderTab(tab);
}
function filterNet(tab,val){filterState[tab].network=val;renderTab(tab);}
function filterSearch(tab,val){filterState[tab].search=val;renderTab(tab);}

// ── Refresh ───────────────────────────────────────────────────────────────────
async function triggerRefresh() {
  document.querySelectorAll('[id$="-refresh-btn"]').forEach(b=>b.classList.add('spinning'));
  document.getElementById('status-text').textContent='Refreshing…';
  try { await fetch('/api/refresh',{method:'POST'}); } catch(e){}
  const prev=JSON.stringify(lastUpdated);
  let attempts=0;
  const timer=setInterval(async()=>{
    attempts++;
    try {
      const res=await fetch('/api/data');
      const json=await res.json();
      if(JSON.stringify(json.last_updated)!==prev){
        clearInterval(timer);
        onData(json);
        document.querySelectorAll('[id$="-refresh-btn"]').forEach(b=>b.classList.remove('spinning'));
      }
    } catch(e){}
    if(attempts>=60){clearInterval(timer);document.querySelectorAll('[id$="-refresh-btn"]').forEach(b=>b.classList.remove('spinning'));}
  },2000);
}

// ── Data reception ────────────────────────────────────────────────────────────
function onData(json) {
  appData    = json.data||{};
  lastUpdated= json.last_updated||{};
  companySlugs=(json.companies||Object.keys(appData)).filter(s=>appData[s]);
  companySlugs.forEach((s,i)=>{ _coColorMap[s]=CO_COLORS[i%CO_COLORS.length]; });

  const totalNets=companySlugs.reduce((n,s)=>n+((appData[s]||{}).networks||[]).length,0);
  document.getElementById('co-count').textContent=
    companySlugs.length ? companySlugs.length+' compan'+(companySlugs.length>1?'ies':'y')+' · '+totalNets+' network'+(totalNets!==1?'s':'') : '';

  const ts=Object.values(lastUpdated).sort().pop();
  document.getElementById('status-text').textContent=ts?'Updated '+new Date(ts).toLocaleTimeString():'—';

  initToolbars();
  renderTab(activeTab);
  if(activeTab==='map-section'&&_mapTabInstance) updateMapTab();
}

// ── Render dispatcher ─────────────────────────────────────────────────────────
function renderTab(tab) {
  if(tab==='overview')    { renderOverview(); return; }
  if(tab==='map-section') { onMapTabActivated(); return; }
  if(tab==='net-history') { histInit(); return; }
  if(tab==='config-drift'||tab==='pci-report'||tab==='location-analytics') return;

  const state=filterState[tab];
  let html='';
  const errs=companySlugs.flatMap(s=>(appData[s]||{}).fetchErrors||[]);
  if(errs.length) html+=`<div class="err-banner">⚠ ${errs.slice(0,3).map(esc).join(' · ')}</div>`;
  html+=renderColFilterChips(tab);

  switch(tab){
    case 'uplink-quality': html+=renderUplinkQuality(state,tab); break;
    case 'throughput':     html+=renderThroughput(state,tab);    break;
    case 'clients':        html+=renderClients(state,tab);       break;
    case 'devices':        html+=renderDevices(state,tab);       break;
    case 'ssids':          html+=renderSSIDs(state,tab);         break;
    case 'vlans':          html+=renderVLANs(state,tab);         break;
    case 'switch-ports':   html+=renderSwitchPorts(state,tab);   break;
    case 'events':         html+=renderEvents(state,tab);        break;
  }
  const bodyEl=document.getElementById(tab+'-body');
  if(bodyEl) bodyEl.innerHTML=html;
}

// ── Overview ──────────────────────────────────────────────────────────────────
function setOvCompany(val) { ovCompany=val; renderOverview(); }
function setOvView(v) {
  ovView=v;
  document.getElementById('ov-grid-btn').classList.toggle('active',v==='grid');
  document.getElementById('ov-map-btn').classList.toggle('active',v==='map');
  document.getElementById('ov-grid').style.display=v==='grid'?'':'none';
  document.getElementById('ov-map-wrap').style.display=v==='map'?'':'none';
  if(v==='map') initOvMap();
}

function devCounts(slug, nids) {
  const counts = { AP:0, SWITCH:0, CONTROLLER:0, PDU:0, OTHER:0,
                   online:0, offline:0 };
  for (const nid of nids) {
    for (const d of getData(slug,'virtualDevices',nid)) {
      const t=d.deviceType;
      if(t==='ACCESS_POINT') counts.AP++;
      else if(t==='SWITCH') counts.SWITCH++;
      else if(t==='CONTROLLER') counts.CONTROLLER++;
      else if(t==='POWER_DISTRIBUTION_UNIT') counts.PDU++;
      else counts.OTHER++;
      if(d.isOnline) counts.online++; else counts.offline++;
    }
  }
  return counts;
}

function uplinkHealth(slug, nids) {
  let healthy=0, degraded=0, offline=0;
  for (const nid of nids) {
    const entry=(((appData[slug]||{}).uplinkQuality||{})[nid]);
    if(!entry) continue;
    const byIface={};
    (entry.values||[]).forEach(v=>{
      const k=v.phyInterfaceUUID||'?';
      if(!byIface[k]) byIface[k]=[];
      byIface[k].push(v.value);
    });
    for (const vals of Object.values(byIface)) {
      const latest=vals[vals.length-1];
      if(latest==null||latest===0) offline++;
      else if(latest>0.8) healthy++;
      else degraded++;
    }
  }
  return {healthy,degraded,offline};
}

// Per-type device breakdown: {AP:{healthy,alerting,offline}, SWITCH:{...}, CONTROLLER:{...}, PDU:{...}}
function devCountsByType(slug, nids) {
  const types = {AP:{healthy:0,alerting:0,offline:0}, SWITCH:{healthy:0,alerting:0,offline:0},
                 CONTROLLER:{healthy:0,alerting:0,offline:0}, PDU:{healthy:0,alerting:0,offline:0}};
  // Build a set of network UUIDs with degraded uplinks for alerting classification
  const degradedNets = new Set();
  for (const nid of nids) {
    const entry = (((appData[slug]||{}).uplinkQuality||{})[nid]);
    if (!entry) continue;
    const byIface = {};
    (entry.values||[]).forEach(v=>{const k=v.phyInterfaceUUID||'?';if(!byIface[k])byIface[k]=[];byIface[k].push(v.value);});
    for (const vals of Object.values(byIface)) {
      const latest = vals[vals.length-1];
      if (latest!=null && latest>0 && latest<=0.8) { degradedNets.add(nid); break; }
    }
  }
  for (const nid of nids) {
    const isDegraded = degradedNets.has(nid);
    for (const d of getData(slug,'virtualDevices',nid)) {
      const t = d.deviceType==='ACCESS_POINT'?'AP':d.deviceType==='SWITCH'?'SWITCH':
                d.deviceType==='CONTROLLER'?'CONTROLLER':d.deviceType==='POWER_DISTRIBUTION_UNIT'?'PDU':null;
      if (!t) continue;
      if (!d.isOnline) types[t].offline++;
      else if (isDegraded) types[t].alerting++;
      else types[t].healthy++;
    }
  }
  return types;
}

const _collapsedCos = new Set();
function toggleNetRows(slug) {
  if (_collapsedCos.has(slug)) _collapsedCos.delete(slug);
  else _collapsedCos.add(slug);
  renderOverview();
}

function _devCol(label, counts) {
  const total = counts.healthy + counts.alerting + counts.offline;
  if (!total) return `<div class="dev-col">
    <div class="dev-col-lbl">${label}</div>
    <div style="color:var(--text-muted);font-size:11px">—</div>
  </div>`;
  const h=counts.healthy, a=counts.alerting, o=counts.offline;
  return `<div class="dev-col">
    <div class="dev-col-lbl">${label}</div>
    <div class="dev-counter-row">
      ${h>0?`<span class="dev-ctr online"><span class="dev-ctr-dot g"></span>${h}</span>`:''}
      ${a>0?`<span class="dev-ctr alerting"><span class="dev-ctr-dot y"></span>${a}</span>`:''}
      ${o>0?`<span class="dev-ctr offline"><span class="dev-ctr-dot r"></span>${o}</span>`:''}
      ${h===0&&a===0&&o===0?`<span class="dev-ctr dim">${total}</span>`:''}
    </div>
    <div style="font-size:10px;color:var(--text-muted);margin-top:3px">${total} total</div>
  </div>`;
}

function renderOverview() {
  const slugs = ovCompany==='all' ? companySlugs : [ovCompany];
  const errEl = document.getElementById('ov-err');
  const allErrs = slugs.flatMap(s=>(appData[s]||{}).fetchErrors||[]);
  errEl.innerHTML = allErrs.length ? `<div class="err-banner">⚠ ${allErrs.slice(0,3).map(esc).join(' · ')}</div>` : '';

  const gridEl = document.getElementById('ov-grid');
  if (!slugs.length) { gridEl.innerHTML=empty('No data','Waiting for first refresh…'); return; }

  gridEl.innerHTML = slugs.map(slug => {
    const d = appData[slug]||{};
    const nets = d.networks||[];
    if (!nets.length) return `<div class="co-card"><div class="co-card-hdr">
      <div class="co-badge" style="background:${coColor(slug)}">${esc(coInitial(slug))}</div>
      <div class="co-name">${esc(coLabel(slug))}</div>
      <span style="margin-left:auto;font-size:11px;color:var(--text-muted)">No networks</span>
    </div></div>`;

    const nids = nets.map(n=>n.UUID);
    const dc = devCounts(slug, nids);
    const dct = devCountsByType(slug, nids);
    const uh = uplinkHealth(slug, nids);
    const collapsed = _collapsedCos.has(slug);

    const netRows = nets.map(net => {
      const ndc = devCounts(slug,[net.UUID]);
      const ndct = devCountsByType(slug,[net.UUID]);
      const nuh = uplinkHealth(slug,[net.UUID]);
      const nOnline=ndc.online, nTotal=ndc.AP+ndc.SWITCH+ndc.CONTROLLER+ndc.PDU+ndc.OTHER;
      const hlth = nOnline===nTotal&&nTotal>0?'green':nOnline===0&&nTotal>0?'red':'yellow';
      const addr = net.mailingAddress;
      const addrStr = addr ? [addr.line1,addr.city].filter(Boolean).join(', ') : '';
      const uhTotal = nuh.healthy+nuh.degraded+nuh.offline;
      // Chip: "LABEL ↑up ↓dn" — ↓dn omitted when 0
      const tc = (lbl, ct, cls) => {
        const tot=ct.healthy+ct.alerting+ct.offline; if(!tot) return '';
        const up=ct.healthy+ct.alerting, dn=ct.offline;
        return `<span class="badge ${cls}" style="font-size:10px">${lbl} <span style="color:var(--green)">↑${up}</span>${dn?` <span style="color:var(--red)">↓${dn}</span>`:''}</span>`;
      };
      const uplinkChip = uhTotal ? `<span class="badge ${nuh.offline>0?'red':nuh.degraded>0?'yellow':'green'}" style="font-size:10px">ISP ↑${nuh.healthy+nuh.degraded}${nuh.offline?` <span style="color:var(--red)">↓${nuh.offline}</span>`:''}</span>` : '';
      return `<div class="net-row">
        <span class="dot ${hlth}"></span>
        <span class="net-name">${esc(net.label)}</span>
        ${addrStr?`<span class="net-addr">${esc(addrStr)}</span>`:''}
        <span class="net-chips">
          ${tc('AP',ndct.AP,'blue')}${tc('SW',ndct.SWITCH,'purple')}${tc('FW',ndct.CONTROLLER,'dim')}${tc('PDU',ndct.PDU,'dim')}${uplinkChip}
        </span>
      </div>`;
    }).join('');

    const netCount = nets.length;
    const rowsMaxH = netCount * 40 + 8;

    return `<div class="co-card">
      <div class="co-card-hdr">
        <div class="co-badge" style="background:${coColor(slug)}">${esc(coInitial(slug))}</div>
        <div>
          <div class="co-name">${esc(coLabel(slug))}</div>
          <div style="font-size:11px;color:var(--text-muted);margin-top:1px">${netCount} network${netCount!==1?'s':''}</div>
        </div>
        <div class="co-pill-row">
          ${dc.online>0?`<span class="badge green">${dc.online} online</span>`:''}
          ${dc.offline>0?`<span class="badge red">${dc.offline} offline</span>`:''}
          ${uh.healthy>0?`<span class="badge green">${uh.healthy} healthy uplink${uh.healthy!==1?'s':''}</span>`:''}
          ${uh.degraded>0?`<span class="badge yellow">${uh.degraded} degraded</span>`:''}
          ${uh.offline>0?`<span class="badge red">${uh.offline} uplink offline</span>`:''}
        </div>
      </div>
      <div class="dev-cols">
        ${_devCol('APs', dct.AP)}
        ${_devCol('Switches', dct.SWITCH)}
        ${_devCol('Firewalls', dct.CONTROLLER)}
        ${_devCol('PDUs', dct.PDU)}
      </div>
      <div class="co-card-hdr" style="padding:7px 14px;cursor:pointer" onclick="toggleNetRows('${slug}')">
        <span style="font-size:11px;color:var(--text-secondary);font-weight:500">Networks</span>
        <button class="net-toggle">${collapsed?'▶ Show':'▼ Hide'}</button>
      </div>
      <div class="net-rows-wrap${collapsed?' collapsed':''}" style="max-height:${rowsMaxH}px">
        <div class="co-networks">${netRows}</div>
      </div>
    </div>`;
  }).join('');
}
</script>
<script>
// ── Uplink Quality ────────────────────────────────────────────────────────────
function renderUplinkQuality({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const rows=[];
  for (const net of nets) {
    const slug=net._slug;
    const entry=getDataObj(slug,'uplinkQuality',net.UUID);
    if(!entry.values) continue;
    const byIface={};
    (entry.values||[]).forEach(v=>{const k=v.phyInterfaceUUID||'?';if(!byIface[k])byIface[k]=[];byIface[k].push(v);});
    for (const [ifaceUUID,pts] of Object.entries(byIface)) {
      const ifaces=(getData(slug,'uplinkPhyIfaces',net.UUID)||[]);
      const ifInfo=ifaces.find(i=>i.UUID===ifaceUUID)||{};
      const devs=(getData(slug,'virtualDevices',net.UUID)||[]);
      const devLabel=devs.find(d=>d.UUID===ifInfo.virtualDeviceUUID)?.label||'—';
      const nums=pts.map(p=>p.value).filter(x=>x!=null);
      const v=pts[pts.length-1]?.value;
      const avg=nums.length?nums.reduce((a,b)=>a+b,0)/nums.length:null;
      const qualLabel=v==null?'No Data':v>.8?'Good':v>.5?'Fair':'Poor';
      rows.push({net,slug,ifaceUUID,ifLabel:ifInfo.label||('…'+ifaceUUID.slice(-8)),devLabel,pts,v,avg,qualLabel});
    }
  }
  if(!rows.length) return empty('No uplink quality data');
  storeColVals(tab,'Network',rows.map(r=>r.net.label));
  storeColVals(tab,'Company',rows.map(r=>coLabel(r.slug)));
  storeColVals(tab,'Device',rows.map(r=>r.devLabel));
  storeColVals(tab,'Interface',rows.map(r=>r.ifLabel));
  storeColVals(tab,'Quality',rows.map(r=>r.qualLabel));

  function getCV(r,col){switch(col){
    case 'Network': return r.net.label;case 'Company': return coLabel(r.slug);
    case 'Device': return r.devLabel;case 'Interface': return r.ifLabel;
    case 'Quality': return r.qualLabel;case 'Average': return r.avg;case 'Samples': return r.pts.length;
    default: return null;
  }}
  let filtered=rows.filter(r=>hits([r.ifLabel,r.devLabel,r.net.label,r.qualLabel,coLabel(r.slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(r=>[r.ifLabel,r.devLabel,r.net.label,r.qualLabel,coLabel(r.slug)]));

  const allNums=filtered.flatMap(r=>r.pts.map(p=>p.value)).filter(v=>v!=null);
  const gavg=allNums.length?allNums.reduce((a,b)=>a+b,0)/allNums.length:null;
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Interfaces</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Avg Quality</div><div class="stat-val">${gavg!=null?(gavg*100).toFixed(1)+'%':'—'}</div></div>
    <div class="stat"><div class="stat-lbl">Data Points</div><div class="stat-val">${allNums.length}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'';
  const ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(({net,slug,ifLabel,devLabel,pts,v,avg,qualLabel})=>{
    const cls=v==null?'dim':v>.8?'green':v>.5?'yellow':'red';
    const pct=v!=null?(v*100).toFixed(1)+'%':'—';
    const barW=v!=null?(v*100).toFixed(1):0;
    const col=v==null?'var(--border)':v>.8?'var(--green)':v>.5?'var(--yellow)':'var(--red)';
    return `<tr>
      ${showCo?`<td><span class="badge dim" style="border-color:${coColor(slug)};color:${coColor(slug)}">${esc(coLabel(slug))}</span></td>`:''}
      ${showNt?`<td><span class="badge dim">${esc(net.label)}</span></td>`:''}
      <td style="font-weight:500">${esc(devLabel)}</td>
      <td>${esc(ifLabel)}</td>
      <td><div class="tbar-wrap"><div class="tbar-bg"><div class="tbar-fill" style="width:${barW}%;background:${col}"></div></div>
        <span class="badge ${cls}" style="min-width:52px;justify-content:center">${pct}</span></div></td>
      <td style="color:var(--text-secondary)">${avg!=null?(avg*100).toFixed(1)+'%':'—'}</td>
      <td style="color:var(--text-muted)">${pts.length}</td>
    </tr>`;
  }).join('');
  return `<div id="tbl-uplink-quality"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-uplink-quality','uplink-quality.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Device','Device')}${thEl(tab,'Interface','Interface')}${thEl(tab,'Quality','Latest Quality')}${thEl(tab,'Average','Average')}${thEl(tab,'Samples','Samples')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── Throughput ────────────────────────────────────────────────────────────────
function renderThroughput({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const rows=[]; let globalMax=1;
  for (const net of nets) {
    const slug=net._slug;
    const entry=getDataObj(slug,'uplinkThroughput',net.UUID);
    if(!entry.values) continue;
    if((entry.metadata?.maxValue||0)>globalMax) globalMax=entry.metadata.maxValue;
    const byKey={};
    (entry.values||[]).forEach(v=>{const k=`${v.phyInterfaceUUID}||${v.direction}`;
      if(!byKey[k]) byKey[k]={ifaceUUID:v.phyInterfaceUUID,dir:v.direction,pts:[]};byKey[k].pts.push(v);});
    Object.values(byKey).forEach(g=>{
      const ifaces=getData(slug,'uplinkPhyIfaces',net.UUID)||[];
      const ifInfo=ifaces.find(i=>i.UUID===g.ifaceUUID)||{};
      const devs=getData(slug,'virtualDevices',net.UUID)||[];
      const devLabel=devs.find(d=>d.UUID===ifInfo.virtualDeviceUUID)?.label||'—';
      const nums=g.pts.map(p=>p.value).filter(v=>v!=null);
      const avg=nums.length?nums.reduce((a,b)=>a+b,0)/nums.length:0;
      const peak=nums.length?Math.max(...nums):0;
      const latest=g.pts[g.pts.length-1]?.value??0;
      rows.push({...g,net,slug,ifLabel:ifInfo.label||('…'+(g.ifaceUUID||'').slice(-8)),devLabel,avg,peak,latest});
    });
  }
  if(!rows.length) return empty('No throughput data');
  storeColVals(tab,'Direction',rows.map(r=>r.dir==='upload'?'Upload':'Download'));
  if(showCo) storeColVals(tab,'Company',rows.map(r=>coLabel(r.slug)));
  if(showNt) storeColVals(tab,'Network',rows.map(r=>r.net.label));

  function getCV(r,col){switch(col){
    case 'Company': return coLabel(r.slug);case 'Network': return r.net.label;
    case 'Device': return r.devLabel;case 'Interface': return r.ifLabel;
    case 'Direction': return r.dir==='upload'?'Upload':'Download';
    case 'Latest': return r.latest;case 'Average': return r.avg;case 'Peak': return r.peak;
    default: return null;
  }}
  let filtered=rows.filter(r=>hits([r.ifLabel,r.devLabel,r.dir,r.net.label,coLabel(r.slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  const gPeak=filtered.flatMap(r=>r.pts.map(p=>p.value)).filter(v=>v!=null).reduce((m,v)=>v>m?v:m,0);
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Series</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Peak</div><div class="stat-val">${fmtBps(gPeak)}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'';
  const ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(r=>`<tr>
    ${showCo?`<td><span class="badge dim" style="border-color:${coColor(r.slug)};color:${coColor(r.slug)}">${esc(coLabel(r.slug))}</span></td>`:''}
    ${showNt?`<td><span class="badge dim">${esc(r.net.label)}</span></td>`:''}
    <td style="font-weight:500">${esc(r.devLabel)}</td>
    <td>${esc(r.ifLabel)}</td>
    <td>${r.dir==='upload'?badge('↑ Upload','blue'):badge('↓ Download','green')}</td>
    <td>${tBar(r.latest,globalMax,r.dir)}</td>
    <td style="color:var(--text-secondary)">${fmtBps(r.avg)}</td>
    <td style="color:var(--text-secondary)">${fmtBps(r.peak)}</td>
  </tr>`).join('');
  return `<div id="tbl-throughput"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-throughput','throughput.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Device','Device')}${thEl(tab,'Interface','Interface')}${thEl(tab,'Direction','Direction')}${thEl(tab,'Latest','Latest')}${thEl(tab,'Average','Average')}${thEl(tab,'Peak','Peak')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── Clients ───────────────────────────────────────────────────────────────────
function renderClients({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const all=[];
  for (const net of nets) {
    const slug=net._slug;
    const swMap=getDataObj(slug,'switchClientMap',net.UUID)||{};
    getData(slug,'networkClients',net.UUID).forEach(c=>{
      const swInfo=(!c.isWireless&&c.macAddress)?swMap[c.macAddress.toLowerCase()]:null;
      all.push({...c,_net:net,_slug:slug,
        _typeLabel:c.isWireless?'Wi-Fi':'Wired',
        _deviceName:c.isWireless?(c.accessPoint?.label||''):(swInfo?.switchLabel||''),
        _portNumber:swInfo?.portNumber,
        _vlanName:c.connectedVLAN?.name||''});
    });
  }
  if(!all.length) return empty('No clients');
  storeColVals(tab,'Type',all.map(c=>c._typeLabel));
  storeColVals(tab,'VLAN',all.map(c=>c._vlanName).filter(Boolean));
  storeColVals(tab,'SSID',all.map(c=>c.connectedSSID?.ssid).filter(Boolean));
  if(showCo) storeColVals(tab,'Company',all.map(c=>coLabel(c._slug)));
  if(showNt) storeColVals(tab,'Network',all.map(c=>c._net.label));

  function getCV(c,col){switch(col){
    case 'Name': return c.clientName;case 'IP': return c.ip;case 'MAC': return c.macAddress;
    case 'Type': return c._typeLabel;case 'Signal': return c.signal;
    case 'VLAN': return c._vlanName;case 'SSID': return c.connectedSSID?.ssid;
    case 'Device': return c._deviceName;case 'Port': return c._portNumber;
    case 'Network': return c._net.label;case 'Company': return coLabel(c._slug);
    case 'Last Seen': return c.lastSeen;default: return null;
  }}
  let filtered=all.filter(c=>hits([c.clientName,c.ip,c.macAddress,c.connectedSSID?.ssid,c._vlanName,c._net.label,c._deviceName,coLabel(c._slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(c=>[c.clientName,c.ip,c._vlanName,c.connectedSSID?.ssid,c._deviceName,c._net.label,coLabel(c._slug)].filter(Boolean)));

  const wireless=filtered.filter(c=>c.isWireless).length;
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Total</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Wireless</div><div class="stat-val">${wireless}</div></div>
    <div class="stat"><div class="stat-lbl">Wired</div><div class="stat-val">${filtered.length-wireless}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(c=>{
    const vlan=c.connectedVLAN?`<span class="badge dim">${esc(c.connectedVLAN.name)} <span style="color:var(--text-muted)">${c.connectedVLAN.vlanID}</span></span>`:'—';
    const portInfo=c._portNumber!=null?`<span style="color:var(--text-muted)">Port ${c._portNumber}</span>`:'—';
    return `<tr>
      ${showCo?`<td><span class="badge dim" style="border-color:${coColor(c._slug)};color:${coColor(c._slug)}">${esc(coLabel(c._slug))}</span></td>`:''}
      ${showNt?`<td><span class="badge dim">${esc(c._net.label)}</span></td>`:''}
      <td><span style="font-weight:500;color:#fff">${esc(c.clientName)||'—'}</span></td>
      <td><code class="mono">${esc(c.ip)||'—'}</code></td>
      <td><code class="mono" style="color:var(--text-secondary)">${esc(c.macAddress)}</code></td>
      <td>${c.isWireless?badge('Wi-Fi','blue'):badge('Wired','dim')}</td>
      <td>${signalBars(c.signal)}</td>
      <td>${vlan}</td>
      <td style="color:var(--text-secondary)">${esc(c.connectedSSID?.ssid)||'—'}</td>
      <td style="color:var(--text-secondary)">${esc(c._deviceName)||'—'}</td>
      <td>${portInfo}</td>
      <td style="color:var(--text-muted);white-space:nowrap">${timeAgo(c.lastSeen)}</td>
    </tr>`;
  }).join('');
  return `<div id="tbl-clients"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-clients','clients.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Name','Name')}${thEl(tab,'IP','IP')}${thEl(tab,'MAC','MAC')}${thEl(tab,'Type','Type')}${thEl(tab,'Signal','Signal')}${thEl(tab,'VLAN','VLAN')}${thEl(tab,'SSID','SSID')}${thEl(tab,'Device','AP / Switch')}${thEl(tab,'Port','Port')}${thEl(tab,'Last Seen','Last Seen')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── Devices ───────────────────────────────────────────────────────────────────
function renderDevices({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const all=[];
  for (const net of nets) {
    const slug=net._slug;
    getData(slug,'virtualDevices',net.UUID).forEach(d=>{
      const tl=d.deviceType==='ACCESS_POINT'?'Access Point':d.deviceType==='SWITCH'?'Switch':d.deviceType==='CONTROLLER'?'Controller':d.deviceType==='POWER_DISTRIBUTION_UNIT'?'PDU':(d.deviceType||'—');
      all.push({...d,_net:net,_slug:slug,_typeLabel:tl});
    });
  }
  if(!all.length) return empty('No devices');
  storeColVals(tab,'Type',all.map(d=>d._typeLabel));
  storeColVals(tab,'Status',all.map(d=>d.isOnline?'Online':'Offline'));
  storeColVals(tab,'Model',all.map(d=>d.deviceModel).filter(Boolean));
  if(showCo) storeColVals(tab,'Company',all.map(d=>coLabel(d._slug)));
  if(showNt) storeColVals(tab,'Network',all.map(d=>d._net.label));

  function getCV(d,col){switch(col){
    case 'Device': return d.label;case 'Type': return d._typeLabel;case 'Model': return d.deviceModel;
    case 'Status': return d.isOnline?'Online':'Offline';case 'Uptime': return d.uptime;
    case 'Network': return d._net.label;case 'Company': return coLabel(d._slug);default: return null;
  }}
  let filtered=all.filter(d=>hits([d.label,d.deviceModel,d._typeLabel,d.isOnline?'Online':'Offline',d._net.label,coLabel(d._slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(d=>[d.label,d.deviceModel,d._typeLabel,d._net.label,coLabel(d._slug)].filter(Boolean)));

  const online=filtered.filter(d=>d.isOnline).length;
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Total</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Online</div><div class="stat-val" style="color:var(--green)">${online}</div></div>
    <div class="stat"><div class="stat-lbl">Offline</div><div class="stat-val" style="color:${filtered.length-online>0?'var(--red)':'var(--text-secondary)'}">${filtered.length-online}</div></div>
  </div>`;
  const typeCls=t=>t==='Access Point'?'blue':t==='Switch'?'purple':t==='Controller'?'orange':'dim';
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(d=>`<tr>
    ${showCo?`<td><span class="badge dim" style="border-color:${coColor(d._slug)};color:${coColor(d._slug)}">${esc(coLabel(d._slug))}</span></td>`:''}
    ${showNt?`<td><span class="badge dim">${esc(d._net.label)}</span></td>`:''}
    <td style="font-weight:500;color:#fff">${esc(d.label)||'—'}</td>
    <td>${badge(d._typeLabel,typeCls(d._typeLabel))}</td>
    <td style="color:var(--text-secondary)">${esc(d.deviceModel)||'—'}</td>
    <td><div style="display:flex;align-items:center;gap:7px">${dot(d.isOnline?'green':'red')}${badge(d.isOnline?'Online':'Offline',d.isOnline?'green':'red')}</div></td>
    <td style="color:var(--text-secondary)">${d.isOnline?fmtUptime(d.uptime):'—'}</td>
    <td><code class="mono" style="color:var(--text-muted);font-size:10.5px">${esc(d.UUID)}</code></td>
  </tr>`).join('');
  return `<div id="tbl-devices"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-devices','devices.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Device','Device')}${thEl(tab,'Type','Type')}${thEl(tab,'Model','Model')}${thEl(tab,'Status','Status')}${thEl(tab,'Uptime','Uptime')}${thEl(tab,'UUID','UUID')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── SSIDs ─────────────────────────────────────────────────────────────────────
function renderSSIDs({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const all=[];
  for (const net of nets) {
    const slug=net._slug;
    getData(slug,'ssids',net.UUID).forEach(s=>all.push({...s,_net:net,_slug:slug}));
  }
  if(!all.length) return empty('No SSIDs');
  storeColVals(tab,'Band',all.map(s=>s.band).filter(Boolean));
  storeColVals(tab,'Status',all.map(s=>s.isEnabled?'Enabled':'Disabled'));
  if(showCo) storeColVals(tab,'Company',all.map(s=>coLabel(s._slug)));
  if(showNt) storeColVals(tab,'Network',all.map(s=>s._net.label));

  function getCV(s,col){switch(col){
    case 'SSID': return s.ssid;case 'Band': return s.band;
    case 'Status': return s.isEnabled?'Enabled':'Disabled';
    case 'Network': return s._net.label;case 'Company': return coLabel(s._slug);default: return null;
  }}
  let filtered=all.filter(s=>hits([s.ssid,s.band,s._net.label,coLabel(s._slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(s=>[s.ssid,s.band,s._net.label,coLabel(s._slug)].filter(Boolean)));

  const enabled=filtered.filter(s=>s.isEnabled).length;
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Total SSIDs</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Enabled</div><div class="stat-val" style="color:var(--green)">${enabled}</div></div>
    <div class="stat"><div class="stat-lbl">Disabled</div><div class="stat-val" style="color:var(--text-secondary)">${filtered.length-enabled}</div></div>
  </div>`;
  const bandLabel=b=>b?b.replace('BAND_','').replace('_',' ').replace('GHZ',' GHz'):b;
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(s=>`<tr>
    ${showCo?`<td><span class="badge dim" style="border-color:${coColor(s._slug)};color:${coColor(s._slug)}">${esc(coLabel(s._slug))}</span></td>`:''}
    ${showNt?`<td><span class="badge dim">${esc(s._net.label)}</span></td>`:''}
    <td style="font-weight:500;color:#fff">${esc(s.ssid)||'—'}</td>
    <td>${s.band?badge(bandLabel(s.band),'blue'):'—'}</td>
    <td>${s.isEnabled?badge('Enabled','green'):badge('Disabled','dim')}</td>
    <td><code class="mono" style="color:var(--text-muted);font-size:10.5px">${esc(s.UUID)}</code></td>
  </tr>`).join('');
  return `<div id="tbl-ssids"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-ssids','ssids.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'SSID','SSID')}${thEl(tab,'Band','Band')}${thEl(tab,'Status','Status')}${thEl(tab,'UUID','UUID')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── VLANs ─────────────────────────────────────────────────────────────────────
function renderVLANs({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const all=[];
  for (const net of nets) {
    const slug=net._slug;
    getData(slug,'vlans',net.UUID).forEach(v=>all.push({...v,_net:net,_slug:slug}));
  }
  if(!all.length) return empty('No VLANs');
  storeColVals(tab,'Status',all.map(v=>v.isEnabled?'Enabled':'Disabled'));
  storeColVals(tab,'Assignment',all.map(v=>v.ipV4ClientAssignmentProtocol).filter(Boolean));
  if(showCo) storeColVals(tab,'Company',all.map(v=>coLabel(v._slug)));
  if(showNt) storeColVals(tab,'Network',all.map(v=>v._net.label));

  function getCV(v,col){switch(col){
    case 'Name': return v.name;case 'ID': return v.vlanID;
    case 'Status': return v.isEnabled?'Enabled':'Disabled';
    case 'Gateway': return v.ipV4ClientGateway;case 'Prefix': return v.ipV4ClientPrefixLength;
    case 'Assignment': return v.ipV4ClientAssignmentProtocol;
    case 'Network': return v._net.label;case 'Company': return coLabel(v._slug);default: return null;
  }}
  let filtered=all.filter(v=>hits([v.name,String(v.vlanID),v.ipV4ClientGateway,v._net.label,coLabel(v._slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(v=>[v.name,String(v.vlanID),v.ipV4ClientGateway,v._net.label,coLabel(v._slug)].filter(Boolean)));

  const enabled=filtered.filter(v=>v.isEnabled).length;
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Total VLANs</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Enabled</div><div class="stat-val" style="color:var(--green)">${enabled}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(v=>`<tr>
    ${showCo?`<td><span class="badge dim" style="border-color:${coColor(v._slug)};color:${coColor(v._slug)}">${esc(coLabel(v._slug))}</span></td>`:''}
    ${showNt?`<td><span class="badge dim">${esc(v._net.label)}</span></td>`:''}
    <td style="font-weight:500;color:#fff">${esc(v.name)||'—'}</td>
    <td><code class="mono">${v.vlanID??'—'}</code></td>
    <td>${v.isEnabled?badge('Enabled','green'):badge('Disabled','dim')}</td>
    <td><code class="mono" style="color:var(--text-secondary)">${esc(v.ipV4ClientGateway)||'—'}${v.ipV4ClientPrefixLength!=null?'/'+v.ipV4ClientPrefixLength:''}</code></td>
    <td style="color:var(--text-secondary)">${esc(v.ipV4ClientAssignmentProtocol)||'—'}</td>
  </tr>`).join('');
  return `<div id="tbl-vlans"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-vlans','vlans.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Name','Name')}${thEl(tab,'ID','VLAN ID')}${thEl(tab,'Status','Status')}${thEl(tab,'Gateway','Gateway / Prefix')}${thEl(tab,'Assignment','Assignment')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── Switch Ports ──────────────────────────────────────────────────────────────
function renderSwitchPorts({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const allRows=[];
  for (const net of nets) {
    const slug=net._slug;
    const switches=((appData[slug]||{}).switches||{})[net.UUID]||[];
    for (const sw of switches) {
      (sw.ports||[]).forEach(p=>allRows.push({p,sw,net,slug,
        _errLabel:((p.errorRxPackets||0)+(p.errorTxPackets||0))>0?'Yes':'No'}));
    }
  }
  if(!allRows.length) return empty('No switch port data');
  storeColVals(tab,'Switch',allRows.map(r=>r.sw.label));
  storeColVals(tab,'Errors',allRows.map(r=>r._errLabel));
  if(showCo) storeColVals(tab,'Company',allRows.map(r=>coLabel(r.slug)));
  if(showNt) storeColVals(tab,'Network',allRows.map(r=>r.net.label));

  function getCV(r,col){switch(col){
    case 'Company': return coLabel(r.slug);case 'Network': return r.net.label;
    case 'Switch': return r.sw.label;case 'Port': return r.p.portNumber;
    case 'Label': return r.p.label;case 'Speed': return r.p.portSpeedMbps;
    case 'RX Bytes': return r.p.totalRxBytes||0;case 'TX Bytes': return r.p.totalTxBytes||0;
    case 'Errors': return r._errLabel;default: return null;
  }}
  let portRows=allRows.filter(r=>hits([r.sw.label,r.net.label,String(r.p.portNumber),r.p.label,coLabel(r.slug)],q));
  portRows=applyColFilters(portRows,tab,getCV);
  if(!portRows.length) return empty('No results');
  portRows=applySort(portRows,tab,getCV);
  updateAC(tab,portRows.flatMap(r=>[r.sw.label,r.net.label,String(r.p.portNumber),r.p.label,coLabel(r.slug)].filter(Boolean)));

  let totalRx=0,totalTx=0,errPorts=0,maxRx=1,maxTx=1;
  portRows.forEach(({p})=>{
    totalRx+=(p.totalRxBytes||0);totalTx+=(p.totalTxBytes||0);
    if((p.errorRxPackets||0)+(p.errorTxPackets||0)>0) errPorts++;
    if((p.totalRxBytes||0)>maxRx) maxRx=p.totalRxBytes;
    if((p.totalTxBytes||0)>maxTx) maxTx=p.totalTxBytes;
  });
  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Ports</div><div class="stat-val">${portRows.length}</div></div>
    <div class="stat"><div class="stat-lbl">Total RX</div><div class="stat-val">${fmtBytes(totalRx)}</div></div>
    <div class="stat"><div class="stat-lbl">Total TX</div><div class="stat-val">${fmtBytes(totalTx)}</div></div>
    <div class="stat"><div class="stat-lbl">Ports w/ Errors</div><div class="stat-val" style="color:${errPorts>0?'var(--red)':'var(--green)'}">${errPorts}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=portRows.map(({p,sw,net,slug,_errLabel})=>{
    const errCell=_errLabel==='Yes'?`<span class="badge red">RX ${p.errorRxPackets||0} / TX ${p.errorTxPackets||0}</span>`:`<span style="color:var(--text-muted)">—</span>`;
    const speedLbl=p.portSpeedMbps?p.portSpeedMbps+' Mbps':'—';
    const vlanCell=p.nativeVLAN?`<span class="badge dim">${esc(p.nativeVLAN.name)} ${p.nativeVLAN.vlanID}</span>`:'—';
    return `<tr>
      ${showCo?`<td><span class="badge dim" style="border-color:${coColor(slug)};color:${coColor(slug)}">${esc(coLabel(slug))}</span></td>`:''}
      ${showNt?`<td><span class="badge dim">${esc(net.label)}</span></td>`:''}
      <td style="font-weight:500">${esc(sw.label)}</td>
      <td><span style="font-weight:600;color:#fff">${p.portNumber}</span></td>
      <td style="color:var(--text-secondary)">${esc(p.label)||'—'}</td>
      <td>${p.isEnabled!=null?(p.isEnabled?dot('green')+' On':dot('gray')+' Off'):'—'}</td>
      <td style="color:var(--text-secondary)">${speedLbl}</td>
      <td>${vlanCell}</td>
      <td><div class="tbar-wrap"><div class="tbar-bg"><div class="tbar-fill" style="width:${((p.totalRxBytes||0)/maxRx*100).toFixed(1)}%"></div></div>
        <span style="min-width:68px;text-align:right;color:var(--text-primary)">${fmtBytes(p.totalRxBytes||0)}</span></div></td>
      <td><div class="tbar-wrap"><div class="tbar-bg"><div class="tbar-fill up" style="width:${((p.totalTxBytes||0)/maxTx*100).toFixed(1)}%"></div></div>
        <span style="min-width:68px;text-align:right;color:var(--text-primary)">${fmtBytes(p.totalTxBytes||0)}</span></div></td>
      <td>${errCell}</td>
    </tr>`;
  }).join('');
  return `<div id="tbl-switch-ports"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-switch-ports','switch-ports.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Switch','Switch')}${thEl(tab,'Port','Port')}${thEl(tab,'Label','Label')}${thEl(tab,'Enabled','Enabled')}${thEl(tab,'Speed','Speed')}${thEl(tab,'VLAN','Native VLAN')}${thEl(tab,'RX Bytes','RX')}${thEl(tab,'TX Bytes','TX')}${thEl(tab,'Errors','Errors')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}

// ── Events ────────────────────────────────────────────────────────────────────
function renderEvents({company:cf,network:nf,search:q},tab) {
  const nets=selectedNets(cf,nf), showCo=isMultiCo(cf), showNt=isMultiNet(cf,nf);
  const all=[];
  for (const net of nets) {
    const slug=net._slug;
    const entry=getDataObj(slug,'eventLog',net.UUID);
    (entry.events||[]).forEach(e=>all.push({...e,_net:net,_slug:slug}));
  }
  if(!all.length) return empty('No events');
  storeColVals(tab,'Event Type',all.map(e=>e.eventType).filter(Boolean));
  if(showCo) storeColVals(tab,'Company',all.map(e=>coLabel(e._slug)));
  if(showNt) storeColVals(tab,'Network',all.map(e=>e._net.label));

  function getCV(e,col){switch(col){
    case 'Event Type': return e.eventType;case 'API Name': return e.eventTypeAPIName;
    case 'Generated At': return e.generatedAt;case 'Network': return e._net.label;
    case 'Company': return coLabel(e._slug);default: return null;
  }}
  let filtered=all.filter(e=>hits([e.eventType,e.eventTypeAPIName,e._net.label,coLabel(e._slug)],q));
  filtered=applyColFilters(filtered,tab,getCV);
  if(!filtered.length) return empty('No results');
  if(!filterState[tab].sortCol) filtered.sort((a,b)=>new Date(b.generatedAt)-new Date(a.generatedAt));
  else filtered=applySort(filtered,tab,getCV);
  updateAC(tab,filtered.flatMap(e=>[e.eventType,e.eventTypeAPIName,e._net.label,coLabel(e._slug)].filter(Boolean)));

  const stats=`<div class="stats">
    <div class="stat"><div class="stat-lbl">Events</div><div class="stat-val">${filtered.length}</div></div>
    <div class="stat"><div class="stat-lbl">Networks</div><div class="stat-val">${nets.length}</div></div>
  </div>`;
  const coTh=showCo?thEl(tab,'Company','Company'):'', ntTh=showNt?thEl(tab,'Network','Network'):'';
  const trs=filtered.map(e=>{
    const t=(e.eventType||'').toLowerCase();
    const cls=(t.includes('error')||t.includes('fail')||t.includes('down'))?'red':(t.includes('warn')||t.includes('disconnect'))?'yellow':'green';
    const ts=e.generatedAt?new Date(e.generatedAt):null;
    return `<tr>
      ${showCo?`<td><span class="badge dim" style="border-color:${coColor(e._slug)};color:${coColor(e._slug)}">${esc(coLabel(e._slug))}</span></td>`:''}
      ${showNt?`<td><span class="badge dim">${esc(e._net.label)}</span></td>`:''}
      <td>${badge(e.eventType||'—',cls)}</td>
      <td><code class="mono" style="color:var(--text-secondary)">${esc(e.eventTypeAPIName)||'—'}</code></td>
      <td style="white-space:nowrap;color:var(--text-secondary)">${ts?ts.toLocaleString():'—'}</td>
      <td style="white-space:nowrap;color:var(--text-muted)">${ts?timeAgo(e.generatedAt):'—'}</td>
    </tr>`;
  }).join('');
  return `<div id="tbl-events"><div class="tbl-actions"><button class="csv-btn" onclick="csvDownload('tbl-events','events.csv')">↓ CSV</button></div>${stats}<div class="tbl-wrap"><table><thead><tr>${coTh}${ntTh}${thEl(tab,'Event Type','Event Type')}${thEl(tab,'API Name','API Name')}${thEl(tab,'Generated At','Time')}${thEl(tab,'Age','Age')}</tr></thead><tbody>${trs}</tbody></table></div></div>`;
}
</script>
<script>
// ── Shared geocoding ──────────────────────────────────────────────────────────
// ── Geocoding — rate-limited parallel with Nominatim 1 req/sec policy ─────────
const _geoCache = {};
let _geoQueue = Promise.resolve();  // serialised queue ensuring 1 req/sec

function _geocodeAddress(addr) {
  if(!addr) return Promise.resolve(null);
  const key = addr.trim().toLowerCase();
  if(key in _geoCache) return Promise.resolve(_geoCache[key]);
  // Chain onto the queue so requests fire sequentially, 1.1s apart
  _geoQueue = _geoQueue.then(()=>new Promise(resolve=>{
    fetch('https://nominatim.openstreetmap.org/search?format=json&limit=1&q='+encodeURIComponent(addr),
      {headers:{'Accept-Language':'en','User-Agent':'MeterDashboard/1.0'}})
    .then(r=>r.ok?r.json():null)
    .then(js=>{
      const ll = (js&&js.length)?[parseFloat(js[0].lat),parseFloat(js[0].lon)]:null;
      _geoCache[key]=ll;
      resolve(ll);
    })
    .catch(()=>{ _geoCache[key]=null; resolve(null); })
    .finally(()=>{ setTimeout(()=>{},1100); });  // pace: resolve first, then wait
  }).then(v=>{ return new Promise(r=>setTimeout(()=>r(v),1100)); }));
  return _geoQueue;
}

// Batch geocode: resolve all in parallel via the same queue, return Map addr→ll
async function _geocodeBatch(nets) {
  const pairs = nets.map(n=>({net:n, addr:_netAddress(n)}));
  const results = await Promise.all(pairs.map(p=>_geocodeAddress(p.addr)));
  return pairs.map((p,i)=>({net:p.net, ll:results[i]})).filter(x=>x.ll);
}

function _netAddress(net) {
  const m = net.mailingAddress;
  if(!m) return null;
  return [m.line1, m.city, m.postalCode].filter(Boolean).join(', ');
}

function _netHealthLabel(net) {
  const entry = getDataObj(net._slug, 'uplinkQuality', net.UUID);
  if(!entry || !entry.values || !entry.values.length) return 'unknown';
  const vals = entry.values.map(v=>v.value).filter(x=>x!=null);
  if(!vals.length) return 'unknown';
  const avg = vals.reduce((a,b)=>a+b,0)/vals.length;
  return avg > 0.8 ? 'good' : avg > 0.5 ? 'fair' : 'poor';
}

// Strict 4-color palette: green/yellow/red/gray
const _MAP_COLORS = {good:'#3ecf6e', fair:'#f59e0b', poor:'#f05252', unknown:'#8b8fa8'};
function _healthFill(net){ return _MAP_COLORS[_netHealthLabel(net)]; }

function _makeMarker(ll, net) {
  const fill = _healthFill(net);
  const dc = devCounts(net._slug, [net.UUID]);
  const dct = devCountsByType(net._slug, [net.UUID]);
  const total = dc.AP+dc.SWITCH+dc.CONTROLLER+dc.PDU+dc.OTHER;
  const online = dc.online;
  const alerting = dct.AP.alerting+dct.SWITCH.alerting+dct.CONTROLLER.alerting+dct.PDU.alerting;
  const offline = dc.offline;
  const score = total>0 ? Math.round((online/total)*100) : 0;
  const scoreColor = score>=80?'#3ecf6e':score>=50?'#f59e0b':'#f05252';

  const icon = L.divIcon({
    html:`<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="10" fill="${fill}" stroke="#0f1117" stroke-width="2"/>
    </svg>`,
    iconSize:[24,24], iconAnchor:[12,12], popupAnchor:[0,-14], className:''
  });

  const addr = _netAddress(net);
  const popup = `<div style="font-family:inherit;min-width:186px;font-size:12px">
    <div style="font-weight:700;margin-bottom:3px;color:#fff">${esc(net.label)}</div>
    ${addr?`<div style="font-size:11px;color:#8b8fa8;margin-bottom:8px">${esc(addr)}</div>`:''}
    <div style="display:flex;gap:8px;margin-bottom:8px">
      <span style="display:flex;align-items:center;gap:4px"><span style="width:8px;height:8px;border-radius:50%;background:#3ecf6e;display:inline-block"></span><b>${online}</b> online</span>
      <span style="display:flex;align-items:center;gap:4px"><span style="width:8px;height:8px;border-radius:50%;background:#f59e0b;display:inline-block"></span><b>${alerting}</b> alerting</span>
      <span style="display:flex;align-items:center;gap:4px"><span style="width:8px;height:8px;border-radius:50%;background:#f05252;display:inline-block"></span><b>${offline}</b> offline</span>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      <div style="flex:1;height:6px;background:#232538;border-radius:3px;overflow:hidden">
        <div style="width:${score}%;height:100%;background:${scoreColor};border-radius:3px"></div>
      </div>
      <span style="font-weight:700;color:${scoreColor};min-width:34px;text-align:right">${score}/100</span>
    </div>
    <div style="display:flex;gap:8px;margin-top:6px;font-size:11px;color:#8b8fa8">
      <span>APs: ${dc.AP}</span><span>SW: ${dc.SWITCH}</span><span>FW: ${dc.CONTROLLER}</span>${dc.PDU?`<span>PDU: ${dc.PDU}</span>`:''}
    </div>
  </div>`;

  const marker = L.marker(ll, {icon, _health: _netHealthLabel(net), _net: net, _dc: dc});
  marker.bindPopup(popup, {closeButton:false, className:'meter-popup', maxWidth:240});
  marker.on('mouseover', function(){ this.openPopup(); });
  marker.on('mouseout',  function(){ this.closePopup(); });
  return marker;
}

function _fitMap(map, bounds) {
  if(!bounds.length) return;
  if(bounds.length===1) map.setView(bounds[0],12);
  else map.fitBounds(bounds, {padding:[40,40], maxZoom:13});
}

// ── Overview Map ──────────────────────────────────────────────────────────────
let _ovMap = null;
let _ovMarkersLayer = null;

// ── Marker cluster factory ────────────────────────────────────────────────────
function _healthColor(h) {
  return h==='poor'?'#f05252':h==='fair'?'#f59e0b':h==='good'?'#3ecf6e':'#8b8fa8';
}

function _makeClusterGroup() {
  const group = L.markerClusterGroup({
    showCoverageOnHover: false,
    spiderfyOnMaxZoom: true,
    zoomToBoundsOnClick: true,
    maxClusterRadius: 55,
    disableClusteringAtZoom: 17,
    iconCreateFunction: function(cluster) {
      const count = cluster.getChildCount();
      // Aggregate health across children: if any child marker is red/yellow, reflect in cluster color
      let worst = 'good';
      const rank = {good:0, fair:1, poor:2, unknown:-1};
      cluster.getAllChildMarkers().forEach(m => {
        const h = m.options._health || 'unknown';
        if(rank[h] > rank[worst]) worst = h;
      });
      const fill = _healthColor(worst);
      const size = count < 10 ? 34 : count < 50 ? 42 : 50;
      return L.divIcon({
        html: `<div style="width:${size}px;height:${size}px;border-radius:50%;
                 background:${fill};opacity:.92;border:3px solid #0f1117;
                 display:flex;align-items:center;justify-content:center;
                 font-family:inherit;font-weight:700;font-size:${count<10?13:count<100?12:11}px;
                 color:#fff;box-shadow:0 2px 8px rgba(0,0,0,.4)">
                 ${count}</div>`,
        className: 'meter-cluster',
        iconSize: [size, size]
      });
    }
  });

  // ── Hover popup on clusters: list underlying networks with their status ───
  group.on('clustermouseover', function(ev) {
    const cluster = ev.layer;
    const children = cluster.getAllChildMarkers();
    // Sort: poor → fair → unknown → good, so issues surface first
    const order = {poor:0, fair:1, unknown:2, good:3};
    children.sort((a,b) => (order[a.options._health]??2) - (order[b.options._health]??2));

    // Summary counts
    let good=0, fair=0, poor=0, unk=0;
    children.forEach(m => {
      const h = m.options._health || 'unknown';
      if(h==='good') good++; else if(h==='fair') fair++;
      else if(h==='poor') poor++; else unk++;
    });

    // Truncate long lists
    const MAX_ROWS = 12;
    const shown = children.slice(0, MAX_ROWS);
    const overflow = children.length - shown.length;

    const rows = shown.map(m => {
      const n  = m.options._net  || {};
      const dc = m.options._dc   || {online:0, offline:0};
      const h  = m.options._health || 'unknown';
      return `<div style="display:flex;align-items:center;gap:6px;padding:3px 0;font-size:11px;
                border-bottom:1px solid rgba(255,255,255,.04)">
        <span style="width:8px;height:8px;border-radius:50%;background:${_healthColor(h)};flex-shrink:0"></span>
        <span style="color:#fff;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:170px">${esc(n.label||'—')}</span>
        <span style="color:#3ecf6e;font-size:10px">↑${dc.online||0}</span>
        ${dc.offline?`<span style="color:#f05252;font-size:10px">↓${dc.offline}</span>`:''}
      </div>`;
    }).join('');

    const summary = [
      good ? `<span style="color:#3ecf6e">${good} good</span>` : null,
      fair ? `<span style="color:#f59e0b">${fair} fair</span>` : null,
      poor ? `<span style="color:#f05252">${poor} poor</span>` : null,
      unk  ? `<span style="color:#8b8fa8">${unk} unknown</span>` : null,
    ].filter(Boolean).join(' · ');

    const html = `<div style="font-family:inherit;min-width:220px;max-width:260px">
      <div style="font-weight:700;color:#fff;margin-bottom:2px;font-size:12px">
        ${children.length} network${children.length!==1?'s':''}
      </div>
      <div style="font-size:10px;color:#8b8fa8;margin-bottom:8px">${summary}</div>
      ${rows}
      ${overflow>0?`<div style="padding-top:6px;font-size:10px;color:#8b8fa8;font-style:italic">… ${overflow} more — zoom in to see all</div>`:''}
      <div style="padding-top:8px;font-size:10px;color:#555870;border-top:1px solid rgba(255,255,255,.06);margin-top:4px">
        Click to zoom in
      </div>
    </div>`;

    cluster.bindPopup(html, {closeButton:false, className:'meter-popup', maxWidth:280}).openPopup();
  });
  group.on('clustermouseout', function(ev) {
    ev.layer.closePopup();
  });

  return group;
}

function initOvMap() {
  if(typeof L === 'undefined') {
    document.getElementById('ov-map-wrap').innerHTML =
      '<div style="padding:24px;color:#f05252">Leaflet failed to load.</div>';
    return;
  }
  try {
    if(!_ovMap) {
      _ovMap = L.map('ov-map-container', {zoomControl:true, scrollWheelZoom:true});
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
        attribution:'© <a href="https://www.openstreetmap.org/copyright">OSM</a>', maxZoom:18
      }).addTo(_ovMap);
      _ovMarkersLayer = _makeClusterGroup().addTo(_ovMap);
    }
    requestAnimationFrame(()=>{ _ovMap.invalidateSize(); updateOvMap(ovCompany); });
  } catch(e) {
    document.getElementById('ov-map-wrap').innerHTML =
      '<div style="padding:24px;color:#f05252">Map error: '+e.message+'</div>';
  }
}

async function updateOvMap(slugFilter) {
  if(!_ovMap) return;
  _ovMarkersLayer.clearLayers();
  const nets = (slugFilter && slugFilter!=='all')
    ? allNetworks().filter(n=>n._slug===slugFilter)
    : allNetworks();
  if(!nets.length) return;
  // Phase 1: render cached coords immediately from server cache
  let serverCache = {};
  try { const r = await fetch('/api/geocode'); if(r.ok){ const j=await r.json(); serverCache=j.nets||{}; } } catch(e){}
  const bounds = [];
  const misses = [];
  nets.forEach(net=>{
    const ll = serverCache[net.UUID];
    if(ll){ _makeMarker(ll, net).addTo(_ovMarkersLayer); bounds.push(ll); }
    else misses.push(net);
  });
  if(bounds.length) _fitMap(_ovMap, bounds);
  // Phase 2: progressively geocode misses via front-end queue
  for(const net of misses){
    const addr = _netAddress(net);
    if(!addr) continue;
    const ll = await _geocodeAddress(addr);
    if(ll){ _makeMarker(ll, net).addTo(_ovMarkersLayer); bounds.push(ll); }
  }
  if(bounds.length) _fitMap(_ovMap, bounds);
}

// override setOvCompany so map updates when company filter changes
function setOvCompany(val) {
  ovCompany = val;
  renderOverview();
  if(ovView === 'map') updateOvMap(val);
}

// ── Map Tab ───────────────────────────────────────────────────────────────────
let _mapTabInstance = null;
let _mapTabLayer    = null;

function onMapTabActivated() {
  if(typeof L === 'undefined') {
    document.getElementById('map-tab-container').innerHTML =
      '<div style="padding:32px;color:#f05252">Leaflet failed to load.</div>';
    return;
  }
  try {
    if(!_mapTabInstance) {
      _mapTabInstance = L.map('map-tab-container', {zoomControl:true, scrollWheelZoom:true});
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
        attribution:'© <a href="https://www.openstreetmap.org/copyright">OSM</a>', maxZoom:18
      }).addTo(_mapTabInstance);
      _mapTabLayer = _makeClusterGroup().addTo(_mapTabInstance);
    }
    requestAnimationFrame(()=>{ _mapTabInstance.invalidateSize(); updateMapTab(); });
  } catch(e) {
    document.getElementById('map-tab-container').innerHTML =
      '<div style="padding:32px;color:#f05252">Map error: '+e.message+'</div>';
  }
}

async function updateMapTab() {
  const statusEl = document.getElementById('map-tab-status');
  if(!_mapTabInstance) return;
  _mapTabLayer.clearLayers();
  const cf = (document.getElementById('map-co-filter')||{}).value || 'all';
  const nets = cf==='all' ? allNetworks() : allNetworks().filter(n=>n._slug===cf);
  if(!nets.length){ if(statusEl) statusEl.textContent='No networks'; return; }
  if(statusEl) statusEl.textContent='Loading cached locations…';
  // Phase 1: render cached coords immediately from server cache
  let serverCache = {};
  try { const r = await fetch('/api/geocode'); if(r.ok){ const j=await r.json(); serverCache=j.nets||{}; } } catch(e){}
  const bounds = [];
  const misses = [];
  nets.forEach(net=>{
    const ll = serverCache[net.UUID];
    if(ll){ _makeMarker(ll, net).addTo(_mapTabLayer); bounds.push(ll); }
    else misses.push(net);
  });
  if(bounds.length){ _fitMap(_mapTabInstance, bounds); }
  if(statusEl) statusEl.textContent = bounds.length+' placed'+(misses.length?`, geocoding ${misses.length} more…`:'');
  // Phase 2: progressively geocode misses via front-end queue
  for(const net of misses){
    const addr = _netAddress(net);
    if(!addr) continue;
    const ll = await _geocodeAddress(addr);
    if(ll){ _makeMarker(ll, net).addTo(_mapTabLayer); bounds.push(ll); }
  }
  if(bounds.length) _fitMap(_mapTabInstance, bounds);
  if(statusEl) statusEl.textContent = bounds.length+' network'+(bounds.length!==1?'s':'')+' placed';
}

// ── Polling ───────────────────────────────────────────────────────────────────
async function poll() {
  try {
    const res = await fetch('/api/data');
    if(!res.ok) throw new Error('HTTP '+res.status);
    onData(await res.json());
  } catch(e) {
    const st = document.getElementById('status-text');
    if(st) st.textContent='Error: '+e.message;
  }
}

poll();
setInterval(poll, 30000);

// ── CSV Download ──────────────────────────────────────────────────────────────
function csvDownload(id, filename) {
  const tbl = document.querySelector('#'+id+' table');
  if(!tbl){ alert('No table found'); return; }
  const rows = [];
  tbl.querySelectorAll('tr').forEach(tr=>{
    const cells=[];
    tr.querySelectorAll('th,td').forEach(c=>{
      let t = c.innerText.replace(/\n+/g,' ').trim();
      if(t.includes(',')||t.includes('"')||t.includes('\n')) t='"'+t.replace(/"/g,'""')+'"';
      cells.push(t);
    });
    rows.push(cells.join(','));
  });
  const blob = new Blob([rows.join('\r\n')], {type:'text/csv'});
  const a = Object.assign(document.createElement('a'),{href:URL.createObjectURL(blob),download:filename||'export.csv'});
  a.click(); URL.revokeObjectURL(a.href);
}

// ── Config Drift ──────────────────────────────────────────────────────────────
function driftPopulateSelects() {
  ['a','b'].forEach(side=>{
    const coSel=document.getElementById('drift-co-'+side);
    if(!coSel) return;
    const prev=coSel.value;
    coSel.innerHTML='<option value="">— Company —</option>'+
      companySlugs.map(s=>`<option value="${s}"${prev===s?' selected':''}>${esc(coLabel(s))}</option>`).join('');
    driftPopulateNetSel(side);
  });
}
function driftPopulateNetSel(side) {
  const coSel=document.getElementById('drift-co-'+side);
  const netSel=document.getElementById('drift-net-'+side);
  if(!coSel||!netSel) return;
  const slug=coSel.value;
  const nets=slug?((appData[slug]||{}).networks||[]):[];
  const prev=netSel.value;
  netSel.innerHTML='<option value="">— Network —</option>'+
    nets.map(n=>`<option value="${n.UUID}"${prev===n.UUID?' selected':''}>${esc(n.label)}</option>`).join('');
}
document.getElementById('drift-co-a')?.addEventListener('change',()=>{ driftPopulateNetSel('a'); driftRefresh(); });
document.getElementById('drift-co-b')?.addEventListener('change',()=>{ driftPopulateNetSel('b'); driftRefresh(); });

function driftToggleRow(id) {
  const wrap=document.getElementById('drift-r-'+id);
  if(wrap) wrap.classList.toggle('open');
}

function driftRefresh() {
  const slugA=document.getElementById('drift-co-a')?.value;
  const nidA=document.getElementById('drift-net-a')?.value;
  const slugB=document.getElementById('drift-co-b')?.value;
  const nidB=document.getElementById('drift-net-b')?.value;
  const body=document.getElementById('drift-body');
  const sumRow=document.getElementById('drift-summary-row');
  if(!slugA||!nidA||!slugB||!nidB){ body.innerHTML=''; sumRow.style.display='none'; return; }

  const netA=(appData[slugA]?.networks||[]).find(n=>n.UUID===nidA)||{label:nidA};
  const netB=(appData[slugB]?.networks||[]).find(n=>n.UUID===nidB)||{label:nidB};

  const vlansA=getData(slugA,'vlans',nidA);
  const vlansB=getData(slugB,'vlans',nidB);
  const ssidsA=getData(slugA,'ssids',nidA);
  const ssidsB=getData(slugB,'ssids',nidB);

  const allVlanIDs=[...new Set([...vlansA.map(v=>v.vlanID),...vlansB.map(v=>v.vlanID)])].sort((a,b)=>a-b);
  const allSSIDs=[...new Set([...ssidsA.map(s=>s.ssid),...ssidsB.map(s=>s.ssid)])].sort();

  let vlanMatch=0,vlanMiss=0,ssidMatch=0,ssidMiss=0;

  // Helper: one table row comparing a field across both networks
  function cmpRow(label, va, vb) {
    const diff = va!==vb;
    return `<tr><td>${label}</td><td>${va??'—'}</td><td class="${diff?'val-diff':''}">${vb??'—'}</td></tr>`;
  }

  const vlanRows=allVlanIDs.map(id=>{
    const a=vlansA.find(v=>v.vlanID===id);
    const b=vlansB.find(v=>v.vlanID===id);
    const inBoth=!!(a&&b);
    if(inBoth) vlanMatch++; else vlanMiss++;
    const diffCls=inBoth?'drift-match':'drift-miss';
    const diffLbl=inBoth?'✓ Both':(a?'← A only':'→ B only');
    const hasDiffs=inBoth&&(a.name!==b.name||a.ipV4ClientGateway!==b.ipV4ClientGateway||
      a.ipV4ClientPrefixLength!==b.ipV4ClientPrefixLength||
      a.ipV4ClientAssignmentProtocol!==b.ipV4ClientAssignmentProtocol||a.isEnabled!==b.isEnabled);
    const rowId='vlan-'+id;
    const detail=`<div class="drift-detail">
      <table class="drift-cmp-tbl">
        <tr><td></td><td style="font-weight:600;color:var(--text-secondary);padding-bottom:6px">${esc(netA.label)}</td><td style="font-weight:600;color:var(--text-secondary);padding-bottom:6px">${esc(netB.label)}</td></tr>
        ${cmpRow('Name',a?esc(a.name):'—',b?esc(b.name):'—')}
        ${cmpRow('Gateway',a?(a.ipV4ClientGateway||'—'):'—',b?(b.ipV4ClientGateway||'—'):'—')}
        ${cmpRow('Prefix',a?(a.ipV4ClientPrefixLength!=null?'/'+a.ipV4ClientPrefixLength:'—'):'—',b?(b.ipV4ClientPrefixLength!=null?'/'+b.ipV4ClientPrefixLength:'—'):'—')}
        ${cmpRow('DHCP',a?(a.ipV4ClientAssignmentProtocol||'—'):'—',b?(b.ipV4ClientAssignmentProtocol||'—'):'—')}
        ${cmpRow('Enabled',a?(a.isEnabled?'Yes':'No'):'—',b?(b.isEnabled?'Yes':'No'):'—')}
      </table>
    </div>`;
    return `<div class="drift-row-wrap" id="drift-r-${rowId}">
      <div class="drift-item" onclick="driftToggleRow('${rowId}')">
        <span class="drift-toggle">▶</span>
        <span style="min-width:42px;font-weight:600;color:var(--text-primary)">ID ${id}</span>
        <span style="flex:1;color:var(--text-secondary)">${a?esc(a.name):'—'} / ${b?esc(b.name):'—'}</span>
        ${hasDiffs?`<span class="drift-warn" style="font-size:10px">≠ diff</span>`:''}
        <span class="${diffCls}">${diffLbl}</span>
      </div>${detail}
    </div>`;
  }).join('');

  const ssidRows=allSSIDs.map(name=>{
    const a=ssidsA.find(s=>s.ssid===name);
    const b=ssidsB.find(s=>s.ssid===name);
    const inBoth=!!(a&&b);
    if(inBoth) ssidMatch++; else ssidMiss++;
    const diffCls=inBoth?'drift-match':'drift-miss';
    const diffLbl=inBoth?'✓ Both':(a?'← A only':'→ B only');
    const hasDiffs=inBoth&&(a.isEnabled!==b.isEnabled||a.band!==b.band);
    const rowId='ssid-'+btoa(name).replace(/[^a-zA-Z0-9]/g,'').slice(0,12);
    const bandFmt=v=>v?(v.replace('BAND_','').replace('_GHZ',' GHz').replace('_',' ')):'—';
    const detail=`<div class="drift-detail">
      <table class="drift-cmp-tbl">
        <tr><td></td><td style="font-weight:600;color:var(--text-secondary);padding-bottom:6px">${esc(netA.label)}</td><td style="font-weight:600;color:var(--text-secondary);padding-bottom:6px">${esc(netB.label)}</td></tr>
        ${cmpRow('SSID',a?esc(a.ssid):'—',b?esc(b.ssid):'—')}
        ${cmpRow('Band',a?bandFmt(a.band):'—',b?bandFmt(b.band):'—')}
        ${cmpRow('Enabled',a?(a.isEnabled?'Yes':'No'):'—',b?(b.isEnabled?'Yes':'No'):'—')}
      </table>
    </div>`;
    return `<div class="drift-row-wrap" id="drift-r-${rowId}">
      <div class="drift-item" onclick="driftToggleRow('${rowId}')">
        <span class="drift-toggle">▶</span>
        <span style="flex:1;color:var(--text-secondary)">${esc(name)}</span>
        ${hasDiffs?`<span class="drift-warn" style="font-size:10px">≠ diff</span>`:''}
        <span class="${diffCls}">${diffLbl}</span>
      </div>${detail}
    </div>`;
  }).join('');

  const totalDiffs=(vlanMiss+ssidMiss);
  sumRow.style.display='flex';
  sumRow.innerHTML=`
    <div class="drift-summ"><div class="drift-summ-num" style="color:${vlanMatch?'var(--green)':'var(--text-muted)'}">${vlanMatch}</div><div class="drift-summ-lbl">VLANs match</div></div>
    <div class="drift-summ"><div class="drift-summ-num" style="color:${vlanMiss?'var(--red)':'var(--text-muted)'}">${vlanMiss}</div><div class="drift-summ-lbl">VLAN drift</div></div>
    <div class="drift-summ"><div class="drift-summ-num" style="color:${ssidMatch?'var(--green)':'var(--text-muted)'}">${ssidMatch}</div><div class="drift-summ-lbl">SSIDs match</div></div>
    <div class="drift-summ"><div class="drift-summ-num" style="color:${ssidMiss?'var(--red)':'var(--text-muted)'}">${ssidMiss}</div><div class="drift-summ-lbl">SSID drift</div></div>
    <div class="drift-summ"><div class="drift-summ-num" style="color:${totalDiffs>0?'var(--red)':'var(--green)'}">${totalDiffs}</div><div class="drift-summ-lbl">Total diffs</div></div>`;

  body.innerHTML=`<div class="drift-grid">
    <div class="drift-card">
      <div class="drift-card-hdr">VLANs — ${esc(netA.label)} vs ${esc(netB.label)}</div>
      ${vlanRows||'<div class="drift-item" style="color:var(--text-muted);cursor:default">No VLANs</div>'}
    </div>
    <div class="drift-card">
      <div class="drift-card-hdr">SSIDs — ${esc(netA.label)} vs ${esc(netB.label)}</div>
      ${ssidRows||'<div class="drift-item" style="color:var(--text-muted);cursor:default">No SSIDs</div>'}
    </div>
  </div>`;
}

// Re-populate drift selects whenever data loads
const _origOnData = typeof onData === 'function' ? onData : null;
// Note: onData defined in the earlier script block; we patch it post-load
window.addEventListener('load', ()=>{
  if(typeof companySlugs !== 'undefined') driftPopulateSelects();
});

// ── PCI Compliance (embedded wizard) ─────────────────────────────────────────
let _pciNets=[], _pciSelected=new Set(), _pciSetupData={}, _pciCdeSel={};

async function pciInit() {
  if(_pciNets.length) { pciRenderNetList(); return; }
  document.getElementById('pci-net-list').innerHTML=
    '<div class="loading"><div class="spinner"></div><div class="title">Loading networks…</div></div>';
  try {
    const res=await fetch('/api/pci/networks');
    const j=await res.json();
    if(j.error){ document.getElementById('pci-net-list').innerHTML=`<div class="err-banner">${esc(j.error)}</div>`; return; }
    _pciNets=j.networks||[];
    pciRenderNetList();
  } catch(e){ document.getElementById('pci-net-list').innerHTML=`<div class="err-banner">${esc(e.message)}</div>`; }
}

function pciRenderNetList() {
  const rows=_pciNets.map(n=>`<div class="pci-net-card${_pciSelected.has(n.UUID)?' sel':''}" onclick="pciToggleNet('${n.UUID}')">
    <input type="checkbox" ${_pciSelected.has(n.UUID)?'checked':''} onclick="event.stopPropagation();pciToggleNet('${n.UUID}')" style="accent-color:var(--blue)">
    <div><div style="font-weight:500">${esc(n.label)}</div><div style="font-size:11px;color:var(--text-muted)">${esc(n.slug||n.UUID)}</div></div>
  </div>`).join('');
  document.getElementById('pci-net-list').innerHTML=
    `<div style="display:flex;gap:6px;margin-bottom:10px">
      <button class="btn" style="font-size:11px;padding:3px 9px" onclick="pciSelectAll(true)">All</button>
      <button class="btn" style="font-size:11px;padding:3px 9px" onclick="pciSelectAll(false)">None</button>
    </div>${rows}`;
  document.getElementById('pci-btn-cde').disabled=_pciSelected.size===0;
}
function pciToggleNet(uuid){ _pciSelected.has(uuid)?_pciSelected.delete(uuid):_pciSelected.add(uuid); pciRenderNetList(); }
function pciSelectAll(yes){ _pciNets.forEach(n=>yes?_pciSelected.add(n.UUID):_pciSelected.delete(n.UUID)); pciRenderNetList(); }

async function pciGotoCDE() {
  if(!_pciSelected.size) return;
  pciShowStep(2);
  document.getElementById('pci-cde-config').innerHTML=
    '<div class="loading"><div class="spinner"></div><div class="title">Loading VLANs & SSIDs…</div></div>';
  const res=await fetch('/api/pci/setup',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({networkUUIDs:[..._pciSelected]})});
  _pciSetupData=await res.json();
  [..._pciSelected].forEach(nid=>{ if(!_pciCdeSel[nid]) _pciCdeSel[nid]={vlanUUIDs:new Set(),ssidUUIDs:new Set()}; });
  pciRenderCDE();
}

function pciRenderCDE() {
  const html=[..._pciSelected].map(nid=>{
    const net=_pciNets.find(n=>n.UUID===nid)||{label:nid};
    const d=_pciSetupData[nid]||{vlans:[],ssids:[]};
    const sel=_pciCdeSel[nid]||{vlanUUIDs:new Set(),ssidUUIDs:new Set()};
    const vRows=(d.vlans||[]).map(v=>`<label class="pci-item">
      <input type="checkbox" data-nid="${nid}" data-uuid="${v.UUID}" data-t="vlan" ${sel.vlanUUIDs.has(v.UUID)?'checked':''} onchange="pciToggleCDE(this)" style="accent-color:var(--blue)">
      <div><div style="font-weight:500">${esc(v.name)}</div><div style="font-size:11px;color:var(--text-muted)">VLAN ${v.vlanID}</div></div>
    </label>`).join('')||'<div style="color:var(--text-muted);font-size:11px">None</div>';
    const sRows=(d.ssids||[]).map(s=>`<label class="pci-item">
      <input type="checkbox" data-nid="${nid}" data-uuid="${s.UUID}" data-t="ssid" ${sel.ssidUUIDs.has(s.UUID)?'checked':''} onchange="pciToggleCDE(this)" style="accent-color:var(--blue)">
      <div><div style="font-weight:500">${esc(s.ssid)}</div><div style="font-size:11px;color:var(--text-muted)">${s.encryptionProtocol||'Open'}${s.vlan?' — VLAN '+s.vlan.name:''}</div></div>
    </label>`).join('')||'<div style="color:var(--text-muted);font-size:11px">None</div>';
    return `<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;margin-bottom:12px;overflow:hidden">
      <div style="padding:10px 14px;border-bottom:1px solid var(--border);font-weight:600">${esc(net.label)}</div>
      <div class="pci-cde-cols">
        <div class="pci-cde-col"><div class="pci-cde-lbl">VLANs in CDE</div>${vRows}</div>
        <div class="pci-cde-col"><div class="pci-cde-lbl">SSIDs in CDE</div>${sRows}</div>
      </div>
    </div>`;
  }).join('');
  document.getElementById('pci-cde-config').innerHTML=html;
}
function pciToggleCDE(el){
  const nid=el.dataset.nid,uuid=el.dataset.uuid,t=el.dataset.t;
  const s=t==='vlan'?_pciCdeSel[nid].vlanUUIDs:_pciCdeSel[nid].ssidUUIDs;
  el.checked?s.add(uuid):s.delete(uuid);
}

async function pciGenerateReport() {
  pciShowStep(3);
  document.getElementById('pci-report-content').innerHTML=
    '<div class="loading"><div class="spinner"></div><div class="title">Generating…</div></div>';
  const nets=[..._pciSelected].map(nid=>{
    const net=_pciNets.find(n=>n.UUID===nid)||{label:nid};
    const sel=_pciCdeSel[nid]||{vlanUUIDs:new Set(),ssidUUIDs:new Set()};
    return {uuid:nid,label:net.label,cdeVlanUUIDs:[...sel.vlanUUIDs],cdeSsidUUIDs:[...sel.ssidUUIDs]};
  });
  const res=await fetch('/api/pci/report',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({networks:nets})});
  const data=await res.json();
  pciRenderReport(data);
}

function pciRenderReport(data) {
  const STATUS_COL={PASS:'var(--green)',FAIL:'var(--red)',WARNING:'var(--yellow)',MANUAL:'var(--blue)',INFO:'var(--text-muted)'};
  const html=(data.networks||[]).map(nr=>{
    const s=nr.summary||{};
    const sumBar=`<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
      ${['FAIL','WARNING','PASS','MANUAL','INFO'].map(k=>`<div style="background:var(--bg-card);border:1px solid var(--border);border-radius:6px;padding:8px 14px;text-align:center;min-width:70px">
        <div style="font-size:20px;font-weight:700;color:${STATUS_COL[k]}">${s[k]||0}</div>
        <div style="font-size:10px;text-transform:uppercase;color:var(--text-muted)">${k}</div>
      </div>`).join('')}
    </div>`;
    const findings=(nr.findings||[]).map(f=>{
      const open=f.status==='FAIL'||f.status==='WARNING'?' open':'';
      const items=(f.details||[]).map(d=>`<li>${esc(d)}</li>`).join('');
      const rec=f.recommendation?`<div style="margin-top:8px;padding:8px 10px;background:var(--bg-section);border-radius:4px;font-size:11px;color:var(--yellow)">💡 ${esc(f.recommendation)}</div>`:'';
      return `<div class="pci-finding f-${f.status}">
        <div class="pci-fhdr" onclick="this.nextElementSibling.classList.toggle('open')">
          <span style="font-size:10px;font-weight:600;color:var(--text-muted);font-family:monospace;min-width:80px">${esc(f.id)}</span>
          <span style="flex:1;font-weight:500">${esc(f.title)}</span>
          <span style="padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;background:rgba(0,0,0,.2);color:${STATUS_COL[f.status]||'#fff'}">${f.status}</span>
        </div>
        <div class="pci-fbody${open}">
          <div style="font-size:11px;color:var(--text-secondary);margin:8px 0 4px">${esc(f.summary)}</div>
          <ul class="pci-det-list">${items}</ul>${rec}
        </div>
      </div>`;
    }).join('');
    return `<div style="margin-bottom:24px">
      <div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:14px">
        <h3 style="font-size:14px;font-weight:600">${esc((nr.network||{}).label)}</h3>
        <span style="font-size:11px;color:var(--text-muted)">${esc((nr.network||{}).uuid)}</span>
      </div>
      ${sumBar}${findings}
    </div>`;
  }).join('');
  document.getElementById('pci-report-content').innerHTML=html||'<div class="empty"><div class="title">No data</div></div>';
}

function pciShowStep(n) {
  [1,2,3].forEach(i=>{
    document.getElementById('pci-step-'+i).classList.toggle('active',i===n);
    const btn=document.getElementById('pci-nav-'+i);
    if(btn){ btn.style.opacity=i<=n?'1':'0.4'; btn.disabled=i>n; }
  });
  if(n===1&&!_pciNets.length) pciInit();
}

// ── Location Analytics ────────────────────────────────────────────────────────
let _locHistory=[], _locChart=null;
const _LOC_COLORS=['#6e80f8','#3ecf6e','#f59e0b','#f05252','#a78bfa','#34d399','#fb923c','#38bdf8','#9ca8e8'];

async function locRefresh() {
  const st=document.getElementById('loc-status');
  if(st) st.textContent='Refreshing…';
  try{
    await fetch('/api/location/refresh',{method:'POST'});
    await locPoll();
  }catch(e){if(st) st.textContent='Error: '+e.message;}
}

async function locPoll() {
  try{
    const res=await fetch('/api/location');
    const j=await res.json();
    if(j.error){ document.getElementById('loc-status').textContent='Error: '+j.error; return; }
    _locHistory=j.history||[];
    const st=document.getElementById('loc-status');
    if(st) st.textContent=j.last_updated?'Updated '+new Date(j.last_updated).toLocaleTimeString():'—';
    if(!_locHistory.length && !j.last_updated) {
      if(st) st.textContent='Fetching…';
      await fetch('/api/location/refresh',{method:'POST'});
      setTimeout(locPoll, 2500);
      return;
    }
    locRender(j);
  }catch(e){}
}

function locRender(j) {
  const allAreas=j.all_areas||[];
  const snap=_locHistory.length?_locHistory[_locHistory.length-1]:null;

  // Stats row
  const statsEl=document.getElementById('loc-stats-row');
  if(statsEl){
    const total=snap?allAreas.reduce((s,a)=>s+(snap.area_counts[a]||0),0):0;
    statsEl.innerHTML=`<div class="stat" style="min-width:120px"><div class="stat-lbl">Total Clients</div><div class="stat-val">${total}</div></div>`+
      allAreas.map(a=>`<div class="stat" style="min-width:100px"><div class="stat-lbl" style="font-size:10px">${esc(a)}</div><div class="stat-val" style="font-size:18px">${snap?snap.area_counts[a]||0:'—'}</div></div>`).join('');
  }

  // Chart
  const labels=_locHistory.map(s=>new Date(s.ts).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}));
  const bodyEl=document.getElementById('loc-body');
  if(!bodyEl) return;

  // Build chart HTML if needed
  if(!document.getElementById('loc-chart-canvas')) {
    bodyEl.innerHTML=`<div class="loc-chart-card"><div style="font-size:11px;color:var(--text-muted);margin-bottom:10px">Wireless clients by area over time</div><div class="loc-chart-wrap"><canvas id="loc-chart-canvas"></canvas></div></div>`;
  }

  const datasets=allAreas.map((a,i)=>({
    label:a,
    data:_locHistory.map(s=>s.area_counts[a]||0),
    borderColor:_LOC_COLORS[i%_LOC_COLORS.length],
    backgroundColor:'transparent',
    borderWidth:2,
    pointRadius:_locHistory.length>60?0:2,
    tension:0.3,
  }));

  const canvas=document.getElementById('loc-chart-canvas');
  if(!canvas) return;

  if(_locChart&&document.body.contains(_locChart.canvas)){
    _locChart.data.labels=labels;
    _locChart.data.datasets=datasets;
    _locChart.update('none');
  } else {
    if(_locChart) _locChart.destroy();
    _locChart=new Chart(canvas,{
      type:'line',
      data:{labels,datasets},
      options:{
        responsive:true,maintainAspectRatio:false,animation:{duration:200},
        interaction:{mode:'index',intersect:false},
        plugins:{legend:{position:'top',labels:{color:'#9799ad',font:{size:10},padding:10,boxWidth:10}},
          tooltip:{backgroundColor:'#1e202e',titleColor:'#e4e6f0',bodyColor:'#9799ad',borderColor:'#4e5161',borderWidth:1}},
        scales:{
          x:{ticks:{color:'#9799ad',font:{size:10},maxTicksLimit:10,maxRotation:0},grid:{color:'#343647'}},
          y:{beginAtZero:true,ticks:{color:'#9799ad',font:{size:10},precision:0},grid:{color:'#343647'}},
        },
      },
    });
  }
}

// Wire up nav click for Reports sections
document.querySelectorAll('.nav-item[data-tab]').forEach(el=>{
  el.addEventListener('click',()=>{
    const tab=el.dataset.tab;
    if(tab==='config-drift') setTimeout(()=>driftPopulateSelects(),50);
    if(tab==='pci-report') setTimeout(()=>{ if(!_pciNets.length) pciInit(); },50);
    if(tab==='location-analytics') setTimeout(()=>locPoll(),50);
    if(tab==='net-history') setTimeout(()=>histInit(),50);
  });
});

// ── Network History ───────────────────────────────────────────────────────────
let _histDays=1, _histCharts={}, _histNetUUID='';

function histInit() {
  const coSel=document.getElementById('hist-co-sel');
  if(!coSel) return;
  const prev=coSel.value;
  coSel.innerHTML='<option value="">— Company —</option>'+
    companySlugs.map(s=>`<option value="${s}"${s===prev?' selected':''}>${esc(coLabel(s))}</option>`).join('');
  histPopulateNets();
}

function histPopulateNets() {
  const coSel=document.getElementById('hist-co-sel');
  const netSel=document.getElementById('hist-net-sel');
  if(!coSel||!netSel) return;
  const slug=coSel.value;
  const nets=slug?((appData[slug]||{}).networks||[]):[];
  const prev=netSel.value;
  netSel.innerHTML='<option value="">— Network —</option>'+
    nets.map(n=>`<option value="${n.UUID}"${n.UUID===prev?' selected':''}>${esc(n.label)}</option>`).join('');
  if(prev && nets.find(n=>n.UUID===prev)) histLoad();
}

function histSetRange(days) {
  _histDays=days;
  document.querySelectorAll('.hist-range').forEach(b=>b.classList.toggle('active',+b.textContent.replace('d','').replace('h','')===days||(days===1&&b.textContent==='24h')));
  histLoad();
}

async function histLoad() {
  const uuid=document.getElementById('hist-net-sel')?.value;
  _histNetUUID=uuid||'';
  const body=document.getElementById('hist-body');
  if(!uuid){ body.innerHTML='<div class="empty-state" style="margin-top:40px">Select a network to view historical data</div>'; return; }
  body.innerHTML='<div class="loading"><div class="spinner"></div><div class="title">Loading history…</div></div>';

  try{
    const [snapRes, uplinkRes, evtRes] = await Promise.all([
      fetch(`/api/history/network?uuid=${encodeURIComponent(uuid)}&days=${_histDays}`),
      fetch(`/api/history/uplink?uuid=${encodeURIComponent(uuid)}&days=${_histDays}`),
      fetch(`/api/history/events?uuid=${encodeURIComponent(uuid)}&days=${_histDays}`),
    ]);
    const [snapJ, uplinkJ, evtJ] = await Promise.all([snapRes.json(), uplinkRes.json(), evtRes.json()]);

    const info=document.getElementById('hist-db-info');
    if(info) info.textContent=`${(snapJ.rows||[]).length} device snapshots · ${(uplinkJ.rows||[]).length} uplink points · ${(evtJ.events||[]).length} events stored`;

    histRender(snapJ.rows||[], uplinkJ.rows||[], evtJ.events||[]);
  }catch(e){
    body.innerHTML=`<div class="err-banner">Error loading history: ${esc(e.message)}</div>`;
  }
}

function histRender(snapRows, uplinkRows, events) {
  const body=document.getElementById('hist-body');

  // Destroy old charts
  Object.values(_histCharts).forEach(c=>{ try{ c.destroy(); }catch(e){} });
  _histCharts={};

  if(!snapRows.length && !uplinkRows.length){
    body.innerHTML='<div class="empty-state" style="margin-top:40px">No history yet — data is recorded each poll cycle (~5 min intervals)</div>';
    return;
  }

  body.innerHTML=`<div class="hist-chart-grid">
    <div class="hist-chart-card">
      <div class="hist-chart-title">Device Health</div>
      <div class="hist-chart-wrap"><canvas id="hc-devices"></canvas></div>
    </div>
    <div class="hist-chart-card">
      <div class="hist-chart-title">Client Count</div>
      <div class="hist-chart-wrap"><canvas id="hc-clients"></canvas></div>
    </div>
    <div class="hist-chart-card hist-chart-full">
      <div class="hist-chart-title">Uplink Quality</div>
      <div class="hist-chart-wrap" style="height:200px"><canvas id="hc-uplink"></canvas></div>
    </div>
  </div>
  <div class="hist-chart-card" style="margin-bottom:20px">
    <div class="hist-chart-title">Event History (${events.length})</div>
    <div class="tbl-wrap" style="max-height:320px;overflow-y:auto">
      ${histEventsTable(events)}
    </div>
  </div>`;

  const chartOpts=(yLabel)=>({
    responsive:true, maintainAspectRatio:false, animation:{duration:0},
    interaction:{mode:'index',intersect:false},
    plugins:{legend:{position:'top',labels:{color:'#9799ad',font:{size:10},padding:8,boxWidth:8}},
      tooltip:{backgroundColor:'#1e202e',titleColor:'#e4e6f0',bodyColor:'#9799ad',borderColor:'#4e5161',borderWidth:1}},
    scales:{
      x:{ticks:{color:'#9799ad',font:{size:9},maxTicksLimit:8,maxRotation:0},grid:{color:'#343647'}},
      y:{beginAtZero:true,title:{display:!!yLabel,text:yLabel,color:'#9799ad',font:{size:10}},
         ticks:{color:'#9799ad',font:{size:9},precision:0},grid:{color:'#343647'}},
    },
  });

  const fmtTs=ts=>new Date(ts).toLocaleString([],{month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit'});

  // ── Device Health chart ──────────────────────────────────────────────────────
  if(snapRows.length){
    const labels=snapRows.map(r=>fmtTs(r.ts));
    _histCharts.devices=new Chart(document.getElementById('hc-devices'),{type:'line',
      data:{labels,datasets:[
        {label:'Online',data:snapRows.map(r=>r.devices_online),borderColor:'#3ecf6e',backgroundColor:'rgba(62,207,110,.1)',fill:true,borderWidth:2,pointRadius:0,tension:.3},
        {label:'Offline',data:snapRows.map(r=>r.devices_total-r.devices_online),borderColor:'#f05252',backgroundColor:'rgba(240,82,82,.08)',fill:true,borderWidth:2,pointRadius:0,tension:.3},
      ]},options:chartOpts()});

    const clabels=snapRows.map(r=>fmtTs(r.ts));
    _histCharts.clients=new Chart(document.getElementById('hc-clients'),{type:'line',
      data:{labels:clabels,datasets:[
        {label:'Total',data:snapRows.map(r=>r.clients_total),borderColor:'#6e80f8',backgroundColor:'rgba(110,128,248,.1)',fill:true,borderWidth:2,pointRadius:0,tension:.3},
        {label:'Wireless',data:snapRows.map(r=>r.clients_wireless),borderColor:'#f59e0b',borderWidth:1.5,pointRadius:0,tension:.3,fill:false},
        {label:'Wired',data:snapRows.map(r=>r.clients_total-r.clients_wireless),borderColor:'#8b8fa8',borderWidth:1.5,pointRadius:0,tension:.3,fill:false},
      ]},options:chartOpts()});
  }

  // ── Uplink Quality chart ─────────────────────────────────────────────────────
  if(uplinkRows.length){
    const byIface={};
    uplinkRows.forEach(r=>{
      if(!byIface[r.iface_uuid]) byIface[r.iface_uuid]=[];
      byIface[r.iface_uuid].push({ts:r.ts_bucket,v:r.value});
    });
    const allTs=[...new Set(uplinkRows.map(r=>r.ts_bucket))].sort();
    const PALETTE=['#6e80f8','#3ecf6e','#f59e0b','#f05252','#8b8fa8','#22d3ee'];
    const datasets=Object.entries(byIface).map(([iface,pts],i)=>{
      const byTs=Object.fromEntries(pts.map(p=>[p.ts,p.v]));
      return {label:iface.slice(-8),data:allTs.map(t=>(byTs[t]??null)),
        borderColor:PALETTE[i%PALETTE.length],borderWidth:2,pointRadius:0,tension:.3,fill:false,spanGaps:true};
    });
    _histCharts.uplink=new Chart(document.getElementById('hc-uplink'),{type:'line',
      data:{labels:allTs.map(fmtTs),datasets},
      options:{...chartOpts('%'),scales:{...chartOpts('%').scales,y:{...chartOpts('%').scales.y,max:1,
        ticks:{...chartOpts('%').scales.y.ticks,callback:v=>(v*100).toFixed(0)+'%'}}}}});
  }
}

function histEventsTable(events) {
  if(!events.length) return '<div style="padding:16px;color:var(--text-muted);font-size:12px">No events in this range</div>';
  const rows=events.slice(0,200).map(e=>{
    const t=(e.event_type||'').toLowerCase();
    const cls=(t.includes('error')||t.includes('fail')||t.includes('down'))?'red':(t.includes('warn')||t.includes('disconnect'))?'yellow':'green';
    const ts=e.generated_at?new Date(e.generated_at).toLocaleString():'—';
    return `<tr>
      <td>${badge(e.event_type||'—',cls)}</td>
      <td><code class="mono" style="font-size:10.5px;color:var(--text-muted)">${esc(e.event_type_api_name||'—')}</code></td>
      <td style="white-space:nowrap;color:var(--text-muted)">${ts}</td>
    </tr>`;
  }).join('');
  return `<table class="hist-event-tbl"><thead><tr><th>Type</th><th>API Name</th><th>Time</th></tr></thead><tbody>${rows}</tbody></table>`;
}
</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</body>
</html>
"""


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db_init()
    threading.Thread(target=background_loop, daemon=True).start()
    threading.Thread(target=_loc_fetch, daemon=True).start()
    port = int(os.environ.get("PORT", 8081))
    print(f"Multi-company Meter Dashboard running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
