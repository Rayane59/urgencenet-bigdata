import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = {
    "Casablanca": ("CAS", "GHT-Atlantique", 33.57, -7.59),
    "Rabat": ("RAB", "GHT-Atlantique", 34.02, -6.84),
    "Marrakech": ("MAR", "GHT-Sud", 31.63, -8.00),
    "Tanger": ("TAN", "GHT-Nord", 35.76, -5.83),
}
TYPES = ["CHU", "HOPITAL_REGIONAL", "CLINIQUE", "CENTRE_SANTE"]
DEPARTMENTS = ["Traumatologie", "Cardiologie", "Pédiatrie", "Médecine générale"]


def build_facilities(rng):
    facilities = []
    for city, (_, group, lat, lon) in CITIES.items():
        for index in range(10):
            facilities.append({
                "facility_id": f"F-{len(facilities) + 1:03d}",
                "name": f"{city} - Établissement {index + 1}",
                "city": city, "hospital_group": group, "type": TYPES[min(index // 3 + 1, 3)] if index else "CHU",
                "beds": rng.randint(35, 420),
                "latitude": round(lat + rng.uniform(-0.05, 0.05), 5),
                "longitude": round(lon + rng.uniform(-0.05, 0.05), 5),
            })
    return facilities


def generate(n, output, seed):
    rng = random.Random(seed)
    facilities = build_facilities(rng)
    origin = datetime(2026, 11, 1, tzinfo=timezone.utc)
    output.mkdir(parents=True, exist_ok=True)
    for filename, rows in [("facilities.json", facilities)]:
        with (output / filename).open("w", encoding="utf-8", newline="\n") as stream:
            stream.writelines(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)

    timeline = []
    visit_count, duplicate_count = divmod(n - int(n * 0.02), 4)
    duplicate_count += int(n * 0.02)
    for number in range(1, visit_count + 1):
        facility = rng.choice(facilities)
        arrival = origin + timedelta(seconds=rng.randrange(14 * 86400))
        priority = rng.choices([1, 2, 3, 4, 5], weights=[5, 15, 40, 27, 13])[0]
        wait = rng.randint(3, 13 + 17 * priority)
        transfer = rng.random() < 0.12
        destination = rng.choice([
            f for f in facilities if f != facility
            and (f["hospital_group"] == facility["hospital_group"] or f["type"] == "CHU")
        ]) if transfer else None
        base = {
            "visit_id": f"V-{number:07d}", "patient_id": f"P-{rng.randint(1, 9500):05d}",
            "facility_id": facility["facility_id"], "city": facility["city"],
            "department": rng.choice(DEPARTMENTS),
        }
        states = ["ARRIVED", "TRIAGED", "CARE_STARTED", "TRANSFERRED" if transfer else "DISCHARGED"]
        minutes = [0, rng.randint(1, min(wait, 8)), wait, wait + rng.randint(10, 160)]
        for index, (status, offset) in enumerate(zip(states, minutes)):
            timestamp = arrival + timedelta(minutes=offset)
            event = dict(
                base, status=status, priority=priority if index else None,
                wait_minutes=wait if index == 2 else None,
                desk_id=f"T-{CITIES[facility['city']][0]}-{rng.randint(1, 5):02d}" if index == 1 else None,
                transfer_to=destination["facility_id"] if status == "TRANSFERRED" else None,
                event_time=timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            delay = rng.randint(1, 900) if rng.random() < 0.05 else 0
            timeline.append((timestamp + timedelta(seconds=delay), event))
    for timestamp, event in rng.sample(timeline, duplicate_count):
        timeline.append((timestamp + timedelta(seconds=1), event))
    timeline.sort(key=lambda pair: pair[0])
    with (output / "urgences_history.jsonl").open("w", encoding="utf-8", newline="\n") as stream:
        stream.writelines(json.dumps(event, ensure_ascii=False) + "\n" for _, event in timeline)
    print(f"{len(timeline)} événements, {visit_count} passages, {duplicate_count} doublons, {len(facilities)} établissements")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file"], default="file")
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.n < 8:
        parser.error("--n doit valoir au moins 8")
    generate(args.n, args.output, args.seed)
