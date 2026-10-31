from datetime import datetime, timezone


def log(event, **kwargs):
    ts = datetime.now(timezone.utc).isoformat()
    kv = " ".join([f"{k}={repr(v)}" for k, v in kwargs.items()])
    print(f"[{ts}] {event} {kv}")
