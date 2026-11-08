from datetime import datetime, timedelta


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("Un horodatage avec fuseau UTC est requis.")

    return parsed


def calculate_batch_delays(
    *,
    source_published_at: str,
    collected_at: str,
    postgres_published_at: str,
) -> dict[str, float]:
    source = _parse_utc(source_published_at)
    collected = _parse_utc(collected_at)
    postgres = _parse_utc(postgres_published_at)

    if not source <= collected <= postgres:
        raise ValueError(
            "Chronologie incohérente : source <= collecte <= PostgreSQL requis."
        )

    return {
        "source_to_postgres_seconds": (postgres - source).total_seconds(),
        "collection_to_postgres_seconds": (postgres - collected).total_seconds(),
    }

def measure_batch_freshness(
    *,
    batch_id: str,
    expected_event_ids: list[str],
    present_event_ids: set[str],
    source_published_at: str,
    collected_at: str,
    postgres_published_at: str,
) -> dict:
    if not expected_event_ids:
        raise ValueError("Un lot vide ne peut pas être mesuré.")

    expected = set(expected_event_ids)

    if (
        any(
            not isinstance(event_id, str) or not event_id.strip()
            for event_id in expected_event_ids
        )
        or len(expected) != len(expected_event_ids)
    ):
        raise ValueError("Les event_id attendus doivent être valides et uniques.")

    matched_count = len(expected & present_event_ids)
    complete = matched_count == len(expected)

    result = {
        "batch_id": batch_id,
        "status": "complete" if complete else "incomplete",
        "expected_event_count": len(expected),
        "matched_event_count": matched_count,
        "source_to_postgres_seconds": None,
        "collection_to_postgres_seconds": None,
    }

    if complete:
        result.update(
            calculate_batch_delays(
                source_published_at=source_published_at,
                collected_at=collected_at,
                postgres_published_at=postgres_published_at,
            )
        )

    return result