import os
from collections.abc import Mapping


def get_trigger_options(
    environ: Mapping[str, str] | None = None,
) -> dict[str, bool | str]:
    env = os.environ if environ is None else environ
    mode = env.get("STREAMING_MODE", "available_now").strip().lower()

    if mode == "available_now":
        return {"availableNow": True}

    if mode != "active":
        raise ValueError(
            "STREAMING_MODE doit valoir 'available_now' ou 'active'."
        )

    raw_interval = env.get("STREAMING_INTERVAL_SECONDS", "30")

    try:
        interval = int(raw_interval)
    except ValueError as error:
        raise ValueError(
            "STREAMING_INTERVAL_SECONDS doit être un entier strictement positif."
        ) from error

    if interval <= 0:
        raise ValueError(
            "STREAMING_INTERVAL_SECONDS doit être un entier strictement positif."
        )

    return {"processingTime": f"{interval} seconds"}