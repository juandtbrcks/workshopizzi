"""
Izzi Workshop — genera 1 archivo JSON con ESQUEMA EVOLUCIONADO (2 campos nuevos)
para el ejercicio de schema evolution en Auto Loader.

Campos nuevos vs el esquema base:
  - collector_version : STRING  (versión del colector que emitió el evento)
  - latency_ms        : DOUBLE  (latencia medida, nueva métrica)

Salida: raw_jsonl_evolucion/events_v2.json  (JSON-lines, ~500 eventos, event_date=2026-08-17)
"""
import os, json, random, zlib, hashlib, datetime

OUT = os.path.expanduser("~/izzi_demo/workshop_data/raw_jsonl_evolucion")
SEED = 4242
N = 500
DAY = datetime.date(2026, 8, 17)          # día siguiente a la serie base (08-10..08-16)
COLLECTOR_VERSIONS = ["2.4.0", "2.4.1", "2.5.0"]
r = random.Random(SEED)

PLAZAS = [
    ("CDMX-Norte","Ciudad de Mexico","CDMX",19.54,-99.14),
    ("CDMX-Sur","Ciudad de Mexico","CDMX",19.30,-99.15),
    ("Guadalajara","Guadalajara","Jalisco",20.67,-103.35),
    ("Monterrey","Monterrey","Nuevo Leon",25.68,-100.32),
    ("Puebla","Puebla","Puebla",19.04,-98.20),
    ("Cancun","Cancun","Quintana Roo",21.16,-86.85),
]
PLANS   = ["Internet 100","Internet 200","Doble Play 200","Triple Play 500","Internet 1000"]
MODELS  = ["Technicolor CGA4233","Arris TG3452","Huawei EG8145V5","Nokia G-140W-C"]
FIRMWARES = ["10.2.1.4","11.0.1.9","11.1.0.3"]

def h(s): return int(hashlib.md5(s.encode()).hexdigest(), 16)
def client_ip(sid, d):
    ipn = zlib.crc32(f"{sid}|{d}".encode()) % 4194304
    return f"100.{64 + (ipn // 65536) % 64}.{(ipn // 256) % 256}.{ipn % 256}"

def make_event():
    i = int(r.random() * 2000)
    sid = f"IZZI-{i:08d}"
    hx = hashlib.md5(sid.encode()).hexdigest().upper()
    mac = ":".join(hx[j:j+2] for j in range(0, 12, 2))
    model = MODELS[h(sid+"m") % len(MODELS)]
    plaza_name, ciudad, estado, lat, lon = PLAZAS[h(sid+"z") % len(PLAZAS)]
    eid = "%08x-%04x-%04x-%04x-%012x" % (r.getrandbits(32), r.getrandbits(16),
        r.getrandbits(16), r.getrandbits(16), r.getrandbits(48))
    ts = datetime.datetime.combine(DAY, datetime.time()) + datetime.timedelta(seconds=int(r.random()*86400))
    rv = r.random()
    et = ("docsis_poll" if rv < 0.40 else "dhcp_ack" if rv < 0.65 else "dhcp_release"
          if rv < 0.75 else "radius_start" if rv < 0.88 else "radius_stop")
    src = "cmts" if et == "docsis_poll" else ("dhcp" if et.startswith("dhcp") else "radius")
    is_docsis = et == "docsis_poll"; is_sess = et.startswith("radius")
    cmts = f"CMTS-{plaza_name[:3]}-{(h(mac) % 12) + 1:02d}"
    return {
        "event_id": eid,
        "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "event_type": et, "ingest_source": src, "plaza": plaza_name,
        "network": {
            "client_ip": client_ip(sid, DAY.isoformat()),
            "public_ip": f"187.190.{h(sid) % 256}.{h(eid) % 256}",
            "mac_address": mac, "cmts_name": cmts,
            "nas_ip": f"10.20.{h(cmts) % 256}.{h(mac) % 254 + 1}",
            "dhcp_server": f"dhcp-{plaza_name[:3].lower()}-{h(plaza_name) % 4 + 1:02d}",
            "vlan": h(plaza_name) % 400 + 600,
            "lease_seconds": 86400 if et.startswith("dhcp") else None,
        },
        "equipment": {"modem_mac": mac, "serial": f"SN-{h(sid) % 999999999:09d}",
                      "model": model, "firmware": FIRMWARES[h(sid+"f") % len(FIRMWARES)],
                      "device_type": "ont" if ("Huawei" in model or "Nokia" in model) else "cable_modem"},
        "subscriber": {"subscriber_id": sid, "plan": PLANS[h(sid+"p") % len(PLANS)], "account_status": "active"},
        "docsis_metrics": {
            "downstream_power_dbmv": round(r.random()*14 - 7, 1) if is_docsis else None,
            "upstream_power_dbmv": round(r.random()*15 + 35, 1) if is_docsis else None,
            "snr_db": round(r.random()*12 + 30, 1) if is_docsis else None,
            "channel_utilization_pct": round(r.random()*100, 1) if is_docsis else None,
            "uncorrectable_errors": int(r.random()*50) if is_docsis else None,
        },
        "session": {
            "session_id": f"sess-{eid}" if is_sess else None,
            "bytes_in": int(r.random()*5e8) if is_sess else None,
            "bytes_out": int(r.random()*2e8) if is_sess else None,
            "session_time_s": int(r.random()*14400) if is_sess else None,
        },
        "geo": {"ciudad": ciudad, "estado": estado, "lat": lat, "lon": lon},
        # ---------- CAMPOS NUEVOS (esquema evolucionado) ----------
        "collector_version": COLLECTOR_VERSIONS[h(eid) % len(COLLECTOR_VERSIONS)],
        "latency_ms": round(r.random()*40 + 2, 1),
        # ----------------------------------------------------------
        "event_date": DAY.isoformat(),
    }

os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "events_v2.json"), "w") as f:
    for _ in range(N):
        f.write(json.dumps(make_event(), ensure_ascii=False) + "\n")
print(f"OK -> {N} eventos con esquema evolucionado (+collector_version, +latency_ms) en {OUT}/events_v2.json")
