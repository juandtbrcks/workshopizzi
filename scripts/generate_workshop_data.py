"""
Izzi Workshop — generador de eventos JSON sintéticos (stdlib puro, sin PySpark).
Misma estructura anidada que el caso real (network/equipment/subscriber/docsis_metrics/session/geo).
Hilo clave: client_ip ESTABLE por (suscriptor, día) y CAMBIA entre días -> reto SCD2 IP<->equipo.

Produce (local, para subir al Volume del workshop):
  raw_jsonl/event_date=YYYY-MM-DD/part-00N.json   (JSON-lines, splittable — formato "bueno")
  multiline_sample/events_array.json              (array multilínea — gotcha NO splittable)

Uso: python3 generate_workshop_data.py [out_dir]
"""
import sys, os, json, random, zlib, hashlib, datetime, shutil

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/izzi_demo/workshop_data")
SEED = 42
N_SUBS = 2_000
DAYS = 7
BASE_DATE = datetime.date(2026, 8, 10)          # 2026-08-10 .. 2026-08-16
EVENTS_PER_DAY = 4_000                            # ~28K eventos totales
FILES_PER_DAY = 3
r = random.Random(SEED)

PLAZAS = [
    ("CDMX-Norte","Ciudad de Mexico","CDMX",19.54,-99.14),
    ("CDMX-Sur","Ciudad de Mexico","CDMX",19.30,-99.15),
    ("Guadalajara","Guadalajara","Jalisco",20.67,-103.35),
    ("Monterrey","Monterrey","Nuevo Leon",25.68,-100.32),
    ("Puebla","Puebla","Puebla",19.04,-98.20),
    ("Toluca","Toluca","Estado de Mexico",19.29,-99.66),
    ("Leon","Leon","Guanajuato",21.12,-101.68),
    ("Queretaro","Queretaro","Queretaro",20.59,-100.39),
    ("Merida","Merida","Yucatan",20.97,-89.62),
    ("Tijuana","Tijuana","Baja California",32.51,-117.02),
    ("Veracruz","Veracruz","Veracruz",19.17,-96.13),
    ("Cancun","Cancun","Quintana Roo",21.16,-86.85),
]
PLANS   = ["Internet 100","Internet 200","Doble Play 200","Doble Play 500",
           "Triple Play 500","Internet 1000","Doble Play 1000"]
MODELS  = ["Technicolor CGA4233","Arris TG3452","Askey RTF3505VW",
           "Huawei EG8145V5","Nokia G-140W-C","Sagemcom F5688"]
FIRMWARES = ["10.2.1.4","10.2.2.0","11.0.1.9","9.8.7.2","11.1.0.3"]

def h(s):  # hash entero determinístico
    return int(hashlib.md5(s.encode()).hexdigest(), 16)

# ---- dim suscriptores (1:1 con equipo, MAC fija) ----
subs = []
for i in range(N_SUBS):
    sid = f"IZZI-{i:08d}"
    hx = hashlib.md5(sid.encode()).hexdigest().upper()
    mac = ":".join(hx[j:j+2] for j in range(0, 12, 2))
    model = MODELS[h(sid+"m") % len(MODELS)]
    plaza = PLAZAS[h(sid+"z") % len(PLAZAS)]
    status = "active" if r.random() < 0.93 else ("suspended" if r.random() < 0.5 else "inactive")
    subs.append({
        "subscriber_id": sid, "modem_mac": mac,
        "serial": f"SN-{h(sid) % 999999999:09d}",
        "model": model, "firmware": FIRMWARES[h(sid+"f") % len(FIRMWARES)],
        "device_type": "ont" if ("Huawei" in model or "Nokia" in model) else "cable_modem",
        "plan": PLANS[h(sid+"p") % len(PLANS)], "account_status": status, "plaza": plaza,
    })

def client_ip(sid, d):  # CGNAT 100.64.0.0/10, estable por (suscriptor, día)
    ipn = zlib.crc32(f"{sid}|{d}".encode()) % 4194304
    return f"100.{64 + (ipn // 65536) % 64}.{(ipn // 256) % 256}.{ipn % 256}"

def make_event(d):
    s = subs[int(r.random() * N_SUBS)]
    sid = s["subscriber_id"]; plaza_name, ciudad, estado, lat, lon = s["plaza"]
    eid = "%08x-%04x-%04x-%04x-%012x" % (r.getrandbits(32), r.getrandbits(16),
        r.getrandbits(16), r.getrandbits(16), r.getrandbits(48))
    secs = int(r.random() * 86400)
    ts = datetime.datetime.combine(d, datetime.time()) + datetime.timedelta(seconds=secs)
    rv = r.random()
    et = ("docsis_poll" if rv < 0.40 else "dhcp_ack" if rv < 0.65 else "dhcp_release"
          if rv < 0.75 else "radius_start" if rv < 0.88 else "radius_stop")
    src = "cmts" if et == "docsis_poll" else ("dhcp" if et.startswith("dhcp") else "radius")
    is_docsis = et == "docsis_poll"; is_sess = et.startswith("radius")
    cmts = f"CMTS-{plaza_name[:3]}-{(h(s['modem_mac']) % 12) + 1:02d}"
    return {
        "event_id": eid,
        "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "event_type": et, "ingest_source": src, "plaza": plaza_name,
        "network": {
            "client_ip": client_ip(sid, d.isoformat()),
            "public_ip": f"187.190.{h(sid) % 256}.{h(eid) % 256}",
            "mac_address": s["modem_mac"], "cmts_name": cmts,
            "nas_ip": f"10.20.{h(cmts) % 256}.{h(s['modem_mac']) % 254 + 1}",
            "dhcp_server": f"dhcp-{plaza_name[:3].lower()}-{h(plaza_name) % 4 + 1:02d}",
            "vlan": h(plaza_name) % 400 + 600,
            "lease_seconds": 86400 if et.startswith("dhcp") else None,
        },
        "equipment": {"modem_mac": s["modem_mac"], "serial": s["serial"],
                      "model": s["model"], "firmware": s["firmware"], "device_type": s["device_type"]},
        "subscriber": {"subscriber_id": sid, "plan": s["plan"], "account_status": s["account_status"]},
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
        "event_date": d.isoformat(),
    }

if os.path.exists(OUT): shutil.rmtree(OUT)
os.makedirs(OUT)
total = 0
for di in range(DAYS):
    d = BASE_DATE + datetime.timedelta(days=di)
    day_dir = os.path.join(OUT, "raw_jsonl", f"event_date={d.isoformat()}")
    os.makedirs(day_dir, exist_ok=True)
    evs = [make_event(d) for _ in range(EVENTS_PER_DAY)]
    per = EVENTS_PER_DAY // FILES_PER_DAY
    for fi in range(FILES_PER_DAY):
        chunk = evs[fi*per: (fi+1)*per] if fi < FILES_PER_DAY-1 else evs[fi*per:]
        with open(os.path.join(day_dir, f"part-{fi:03d}.json"), "w") as f:
            for e in chunk:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        total += len(chunk)
    print(f"  {d}: {EVENTS_PER_DAY} eventos")

# muestra multilínea (array JSON pretty) — demuestra gotcha NO splittable
ml_dir = os.path.join(OUT, "multiline_sample"); os.makedirs(ml_dir, exist_ok=True)
ml = [make_event(BASE_DATE) for _ in range(200)]
with open(os.path.join(ml_dir, "events_array.json"), "w") as f:
    json.dump(ml, f, ensure_ascii=False, indent=2)

print(f"TOTAL: {total:,} eventos JSONL + 200 en muestra multilínea -> {OUT}")
