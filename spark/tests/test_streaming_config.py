import pytest

from jobs.streaming_config import get_trigger_options


def test_default_mode_is_available_now():
    assert get_trigger_options({}) == {"availableNow": True}


def test_explicit_available_now():
    assert get_trigger_options(
        {"STREAMING_MODE": "available_now"}
    ) == {"availableNow": True}


def test_active_mode_default_interval():
    assert get_trigger_options(
        {"STREAMING_MODE": "active"}
    ) == {"processingTime": "30 seconds"}


def test_active_mode_custom_interval():
    assert get_trigger_options(
        {
            "STREAMING_MODE": "active",
            "STREAMING_INTERVAL_SECONDS": "10",
        }
    ) == {"processingTime": "10 seconds"}


def test_mode_is_normalized():
    assert get_trigger_options(
        {"STREAMING_MODE": " ACTIVE "}
    ) == {"processingTime": "30 seconds"}


@pytest.mark.parametrize("mode", ["", "continuous", "invalid"])
def test_invalid_mode_is_rejected(mode):
    with pytest.raises(ValueError, match="STREAMING_MODE"):
        get_trigger_options({"STREAMING_MODE": mode})


@pytest.mark.parametrize("interval", ["0", "-1", "1.5", "abc", ""])
def test_invalid_active_interval_is_rejected(interval):
    with pytest.raises(ValueError, match="STREAMING_INTERVAL_SECONDS"):
        get_trigger_options(
            {
                "STREAMING_MODE": "active",
                "STREAMING_INTERVAL_SECONDS": interval,
            }
        )


def test_available_now_ignores_unused_interval():
    assert get_trigger_options(
        {
            "STREAMING_MODE": "available_now",
            "STREAMING_INTERVAL_SECONDS": "invalid",
        }
    ) == {"availableNow": True}


def test_reads_environment(monkeypatch):
    monkeypatch.setenv("STREAMING_MODE", "active")
    monkeypatch.setenv("STREAMING_INTERVAL_SECONDS", "15")

    assert get_trigger_options() == {"processingTime": "15 seconds"}