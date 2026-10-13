# ShipGuard

Logistics data quality and anomaly detection system built with Python and SQLite. Work in progress.

## Faults injected

- **duplicate**: the same event is sent twice with the same event_id
- **null_timestamp**: event_time becomes empty (None)
- **time_travel**: event_time is 3 hours earlier than the real time
- **gps_jump**: the latitude changes by 5 to 15 degrees, so the truck seems to jump 500 to 1600 km
- **bad_weight**: the weight is set to -5, 0 or 99999
- **late_event**: the event arrives 10 to 60 seconds late
- **schema_drift**: the field weight_kg is renamed to weightKg
- **clock_skew**: event_time is 2 hours in the future
