import math
from datetime import datetime, timezone


def parse_timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError("Un horodatage avec fuseau est requis.")

    return parsed.astimezone(timezone.utc)


def assess_stage(
    events,
    *,
    started_stage,
    completed_stage,
    now,
    timeout_seconds,
    expected_active=True,
):
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("Un seuil fini strictement positif est requis.")

    current = parse_timestamp(now)
    relevant = []

    for event in events:
        if event.get("stage") not in {
            started_stage,
            completed_stage,
        }:
            continue

        timestamp = parse_timestamp(event["timestamp"])

        if timestamp > current:
            raise ValueError("Un événement est situé dans le futur.")

        relevant.append((timestamp, event))

    relevant.sort(key=lambda item: item[0])

    result = {
        "status": "unknown",
        "last_event_stage": None,
        "last_event_at": None,
        "age_seconds": None,
        "timeout_seconds": timeout_seconds,
    }

    if relevant:
        timestamp, event = relevant[-1]
        result.update({
            "last_event_stage": event["stage"],
            "last_event_at": timestamp.isoformat(),
            "age_seconds": (current - timestamp).total_seconds(),
        })

    if not expected_active:
        result["status"] = "inactive"
        return result

    if not relevant:
        return result

    age = result["age_seconds"]
    unfinished = result["last_event_stage"] == started_stage

    if unfinished:
        result["status"] = (
            "overdue" if age > timeout_seconds else "running"
        )
    else:
        result["status"] = (
            "stale" if age > timeout_seconds else "waiting"
        )

    return result