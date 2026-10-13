"""ShipGuard local simulator: Python + SQLite only (no Docker needed).

It creates fake shipment events, secretly breaks some of them (faults),
and saves them into a small database file called shipguard.db.

  raw_events    -> what your pipeline will read (like a Kafka topic)
  ground_truth  -> the hidden answer key, used ONLY to measure accuracy later

Run:  python simulator/local_stream.py --eps 5 --seconds 30
Stop early any time with Ctrl+C.
"""
import argparse
import json
import random
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone

CITIES = {
    "Hyderabad": (17.385, 78.487), "Bengaluru": (12.972, 77.594),
    "Chennai": (13.083, 80.270), "Mumbai": (19.076, 72.878),
    "Delhi": (28.614, 77.209), "Kolkata": (22.573, 88.364),
    "Pune": (18.520, 73.857), "Vijayawada": (16.506, 80.648),
}
STAGES = ["CREATED", "PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED"]
DEVICES = [f"SCN-{i:02d}" for i in range(1, 11)]
FAULTS = ["duplicate", "null_timestamp", "time_travel", "gps_jump",
          "bad_weight", "late_event", "schema_drift", "clock_skew"]
DRIFTING_DEVICE = "SCN-07"   # slowly miscalibrates (the hard, gradual fault)


class Shipment:
    def __init__(self):
        self.id = "SHP-" + uuid.uuid4().hex[:8].upper()
        self.origin, self.dest = random.sample(list(CITIES), 2)
        self.weight = round(random.uniform(1, 500), 2)
        self.stage = 0


def make_event(s, now, drift_factor):
    frac = s.stage / (len(STAGES) - 1)
    (la1, lo1), (la2, lo2) = CITIES[s.origin], CITIES[s.dest]
    device = random.choice(DEVICES)
    weight = s.weight * (drift_factor if device == DRIFTING_DEVICE else 1.0)
    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "shipment_id": s.id,
        "event_type": STAGES[s.stage],
        "event_time": now.isoformat(),
        "device_id": device,
        "origin": s.origin,
        "destination": s.dest,
        "lat": round(la1 + (la2 - la1) * frac + random.gauss(0, 0.01), 5),
        "lon": round(lo1 + (lo2 - lo1) * frac + random.gauss(0, 0.01), 5),
        "weight_kg": round(weight, 2),
    }


def inject_fault(ev, fault, now):
    """Break the event in a specific way. Returns (events_to_send, delay_seconds)."""
    delay = 0
    if fault == "null_timestamp":
        ev["event_time"] = None
    elif fault == "time_travel":            # stage appears to happen 3 hours earlier
        ev["event_time"] = (now - timedelta(hours=3)).isoformat()
    elif fault == "clock_skew":             # device clock is 2 hours ahead
        ev["event_time"] = (now + timedelta(hours=2)).isoformat()
    elif fault == "gps_jump":               # truck "teleports" 500-1600 km
        ev["lat"] += random.choice([-1, 1]) * random.uniform(5, 15)
    elif fault == "bad_weight":
        ev["weight_kg"] = random.choice([-5, 0, 99999])
    elif fault == "schema_drift":           # field silently renamed
        ev["weightKg"] = ev.pop("weight_kg")
    elif fault == "late_event":             # arrives 10-60 seconds late
        delay = random.uniform(10, 60)
    copies = [ev, dict(ev)] if fault == "duplicate" else [ev]
    return copies, delay


def setup_db(path):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS raw_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            payload     TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ground_truth (
            event_id   TEXT,
            fault      TEXT NOT NULL,
            emitted_at TEXT
        );
    """)
    db.commit()
    return db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="shipguard.db")
    ap.add_argument("--eps", type=float, default=5, help="events per second")
    ap.add_argument("--seconds", type=int, default=30, help="how long to run")
    ap.add_argument("--fault-rate", type=float, default=0.08)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    db = setup_db(args.db)
    active, delayed = [], []                 # delayed: (release_time, event, label)
    start = time.time()
    sent = 0

    def emit(ev, label):
        nonlocal sent
        stamp = datetime.now(timezone.utc).isoformat()
        db.execute("INSERT INTO raw_events (received_at, payload) VALUES (?, ?)",
                   (stamp, json.dumps(ev)))
        db.execute("INSERT INTO ground_truth VALUES (?, ?, ?)",
                   (ev["event_id"], label, stamp))
        db.commit()
        sent += 1
        flag = "" if label == "none" else f"   <-- FAULT: {label}"
        print(f"{sent:>4}  {ev['shipment_id']}  {ev['event_type']:<17}{flag}")

    print(f"Running for {args.seconds}s at {args.eps} events/sec. Saving to {args.db}")
    try:
        while time.time() - start < args.seconds:
            t = time.time()
            now = datetime.now(timezone.utc)

            for item in [d for d in delayed if d[0] <= t]:   # release late events
                delayed.remove(item)
                emit(item[1], item[2])

            if len(active) < 50 or random.random() < 0.1:
                active.append(Shipment())
            s = random.choice(active)

            drift = min(1.5, 1 + 0.002 * (t - start))        # +0.2% per second
            ev = make_event(s, now, drift)
            label = "sensor_drift" if (ev["device_id"] == DRIFTING_DEVICE and drift > 1.25) else "none"

            if random.random() < args.fault_rate:
                label = random.choice(FAULTS)
                copies, delay = inject_fault(ev, label, now)
            else:
                copies, delay = [ev], 0

            for c in copies:
                if delay:
                    delayed.append((t + delay, c, label))
                else:
                    emit(c, label)

            s.stage += 1
            if s.stage >= len(STAGES):
                active.remove(s)

            time.sleep(max(0, 1 / args.eps - (time.time() - t)))
    except KeyboardInterrupt:
        print("\nStopped by you.")
    finally:
        db.close()
        print(f"Done. {sent} events saved in {args.db}")


if __name__ == "__main__":
    main()