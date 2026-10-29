import signal
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import run_active_real_ingestion as cli


@pytest.fixture
def setup(tmp_path, monkeypatch):
    validate = Mock()
    callbacks = Mock(return_value=(Mock(), Mock()))
    session = Mock(return_value={
        "status": "session_completed",
        "kafka_enabled": True,
    })

    monkeypatch.setattr(cli, "validate_java", validate)
    monkeypatch.setattr(cli, "java_callbacks", callbacks)
    monkeypatch.setattr(cli, "run_publishing_session", session)

    args = [
        "https://example.invalid/feed",
        str(tmp_path / "collection"),
        str(tmp_path / "gtfs.zip"),
        str(tmp_path / "references"),
    ]
    return args, validate, callbacks, session


def test_execute_is_required(setup):
    args, validate, _, session = setup

    with pytest.raises(SystemExit) as error:
        cli.main(args)

    assert error.value.code == 2
    validate.assert_not_called()
    session.assert_not_called()


def test_success_passes_configuration_to_session(setup):
    args, _, _, session = setup

    code = cli.main(args + [
        "--execute",
        "--cycles", "2",
        "--interval", "120",
        "--limit", "50",
    ])

    assert code == 0
    assert session.call_args.kwargs["cycles"] == 2
    assert session.call_args.kwargs["interval"] == 120
    assert session.call_args.kwargs["limit"] == 50


def test_existing_directory_is_refused(setup):
    args, validate, _, session = setup
    Path(args[1]).mkdir()

    assert cli.main(args + ["--execute"]) == 1

    validate.assert_not_called()
    session.assert_not_called()


def test_java_preflight_failure_prevents_session(setup):
    args, validate, _, session = setup
    validate.side_effect = ValueError("Classe absente")

    assert cli.main(args + ["--execute"]) == 1
    session.assert_not_called()


def test_interrupt_returns_130_and_restores_handlers(setup):
    args, _, _, session = setup
    previous = signal.getsignal(signal.SIGINT)
    session.side_effect = cli.StopRequested(signal.SIGINT)

    assert cli.main(args + ["--execute"]) == 130
    assert signal.getsignal(signal.SIGINT) == previous