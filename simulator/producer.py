"""ShipGuard event simulator with fault injection.

Streams shipment lifecycle events to the `shipment_events` topic.
The hidden truth (which events were corrupted) goes to a SEPARATE
`ground_truth` topic so your pipeline can't "cheat", and you can
later measure precision / recall / detection delay.

Run:  python simulator/producer.py --eps 20 --fault-rate 0.08
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

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
DRIFTING_DEVICE = "SCN-07"   # slowly miscalibrates: the hard, gradual fault


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
    """Mutate the event in place; return the list of events to emit and a delay."""
    delay = 0
    if fault == "null_timestamp":
        ev["event_time"] = None
    elif fault == "time_travel":            # stage happens "before" earlier stages
        ev["event_time"] = (now - timedelta(hours=3)).isoformat()
    elif fault == "clock_skew":             # device clock is ahead
        ev["event_time"] = (now + timedelta(hours=2)).isoformat()
    elif fault == "gps_jump":               # teleport 500-1600 km
        ev["lat"] += random.choice([-1, 1]) * random.uniform(5, 15)
    elif fault == "bad_weight":
        ev["weight_kg"] = random.choice([-5, 0, 99999])
    elif fault == "schema_drift":           # silent rename, version NOT bumped
        ev["weightKg"] = ev.pop("weight_kg")
    elif fault == "late_event":             # arrives out of order
        delay = random.uniform(10, 60)
    copies = [ev, dict(ev)] if fault == "duplicate" else [ev]
    return copies, delay


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--eps", type=float, default=20, help="events per second")
    ap.add_argument("--fault-rate", type=float, default=0.08)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    producer = Producer({"bootstrap.servers": args.broker})
    active, delayed = [], []                 # delayed: (release_time, event, label)
    start = time.time()

    def emit(ev, label):
        producer.produce("shipment_events", key=ev["shipment_id"], value=json.dumps(ev))
        producer.produce("ground_truth", key=ev["event_id"], value=json.dumps({
            "event_id": ev["event_id"], "fault": label,
            "emitted_at": datetime.now(timezone.utc).isoformat()}))

    try:
        while True:
            t = time.time()
            now = datetime.now(timezone.utc)

            for item in [d for d in delayed if d[0] <= t]:   # release late events
                delayed.remove(item)
                emit(item[1], item[2])

            if len(active) < 50 or random.random() < 0.1:
                active.append(Shipment())
            s = random.choice(active)

            drift = min(1.5, 1 + 0.002 * (t - start))        # +0.2% per second, capped
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

            producer.poll(0)
            time.sleep(max(0, 1 / args.eps - (time.time() - t)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
