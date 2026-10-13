import sqlite3
import json
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

db = sqlite3.connect("shipguard.db")
raw = pd.read_sql("SELECT id, payload FROM raw_events", db)
events = pd.json_normalize(raw["payload"].apply(json.loads).tolist())
truth = pd.read_sql("SELECT event_id, fault FROM ground_truth", db).drop_duplicates()
df = events.merge(truth, on="event_id")

show = ["shipment_id", "event_type", "event_time", "lat", "lon", "weight_kg", "weightKg", "fault"]

print("Total events:", len(df))
print()
print("How many of each fault:")
print(df["fault"].value_counts())
print()
print("Faulty events (first 20):")
print(df[df["fault"] != "none"][show].head(20))