import argparse, json, os, random, time
from datetime import datetime, timedelta, timezone
from pathlib import Path


def write_event(path: Path, bridge_id: int, sensor_type: str):
    now = datetime.now(timezone.utc)
    lag = random.randint(0, 60)
    event_time = now - timedelta(seconds=lag)
    ingest_time = datetime.now(timezone.utc)
    if sensor_type == 'temperature':
        value = round(random.uniform(10, 45), 2)
    elif sensor_type == 'vibration':
        value = round(random.uniform(0, 5), 3)
    else:
        value = round(random.uniform(0, 20), 2)
    rec = {
        'event_time': event_time.isoformat(),
        'bridge_id': bridge_id,
        'sensor_type': sensor_type,
        'value': value,
        'ingest_time': ingest_time.isoformat()
    }
    date_dir = event_time.strftime('%Y-%m-%d')
    out_dir = path/ f'dt={date_dir}'
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{sensor_type}_{bridge_id}_{int(time.time()*1000)}.json"
    with open(out_dir / fname, 'w') as f:
        json.dump(rec, f)


def main():
    ap = argparse.ArgumentParser(description='Bridge Monitoring Data Generator (file-based)')
    ap.add_argument('--base', required=True, help='Base folder (e.g., /content/drive/MyDrive/bridge-monitoring)')
    ap.add_argument('--rate-per-5s', type=int, default=3, help='Events per 5 seconds per sensor')
    ap.add_argument('--deterministic', action='store_true', help='Enable deterministic test mode (seed=42)')
    ap.add_argument('--duration', type=int, default=0, help='Run seconds (0 = forever)')
    ap.add_argument('--bridges', type=int, nargs='*', default=[1,2,3,4,5], help='List of bridge_ids')
    args = ap.parse_args()

    if args.deterministic:
        random.seed(42)

    base = Path(args.base)
    temp = base/'streams'/'bridge_temperature'
    vib  = base/'streams'/'bridge_vibration'
    tilt = base/'streams'/'bridge_tilt'
    for p in [temp, vib, tilt]:
        p.mkdir(parents=True, exist_ok=True)

    start = time.time()
    print('Generator started. Ctrl+C to stop.')
    try:
        while True:
            for _ in range(args.rate_per_5s):
                b = random.choice(args.bridges)
                write_event(temp, b, 'temperature')
                write_event(vib,  b, 'vibration')
                write_event(tilt, b, 'tilt')
            time.sleep(5)
            if args.duration and (time.time() - start) >= args.duration:
                break
    except KeyboardInterrupt:
        pass
    print('Generator stopped.')


if __name__ == '__main__':
    main()
