import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import real_active_supervision as runner


@pytest.fixture
def setup(tmp_path, monkeypatch):
    commands = runner.build_real_job_commands(
        tmp_path, tmp_path / "references"
    )
    processes = [Mock(pid=100 + index) for index in range(3)]

    popen = Mock(side_effect=processes)
    stop = Mock(return_value=False)

    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(runner, "stop_process", stop)

    event = Mock()
    event.is_set.return_value = False
    event.wait.return_value = True

    return tmp_path, commands, processes, popen, stop, event


def run(setup):
    project, commands, _, _, _, event = setup

    return runner.supervise_real_jobs(
        project,
        {"PROFILE": "real-test"},
        commands,
        project / "logs",
        event,
    )


def test_commands_use_real_gold_launcher(tmp_path):
    references = tmp_path / "references"

    commands = runner.build_real_job_commands(
        tmp_path, references, gold_interval=60
    )

    assert [name for name, _ in commands] == [
        "bronze", "silver", "gold"
    ]
    assert commands[2][1] == [
        sys.executable,
        str(tmp_path / "scripts/run_active_real_gold.py"),
        str(references),
        "--interval",
        "60",
    ]


def test_starts_three_jobs_and_stops_in_reverse_order(setup):
    _, _, _, popen, stop, _ = setup

    assert run(setup) == 0

    assert popen.call_count == 3
    assert [
        call.args[0] for call in stop.call_args_list
    ] == ["gold", "silver", "bronze"]

    for call in popen.call_args_list:
        assert call.kwargs["start_new_session"] is True
        assert call.kwargs["env"] == {"PROFILE": "real-test"}


def test_unexpected_exit_is_an_error(setup, monkeypatch):
    _, _, _, _, stop, event = setup
    event.wait.return_value = False

    monkeypatch.setattr(
        runner,
        "unexpected_exit",
        Mock(return_value=("silver", 0)),
    )

    assert run(setup) == 1
    assert stop.call_count == 3


def test_partial_startup_failure_stops_started_job(setup):
    _, _, processes, popen, stop, _ = setup
    popen.side_effect = [
        processes[0],
        OSError("Impossible de démarrer Silver"),
    ]

    assert run(setup) == 1

    stop.assert_called_once_with(
        "bronze", processes[0], 60
    )


def test_forced_stop_is_reported_as_failure(setup):
    _, _, _, _, stop, _ = setup
    stop.return_value = True

    assert run(setup) == 1


def test_existing_log_directory_is_not_overwritten(setup):
    project, _, _, popen, _, _ = setup
    (project / "logs").mkdir()

    with pytest.raises(FileExistsError):
        run(setup)

    popen.assert_not_called()

def add_ingestion_job(setup):
    project, commands, processes, popen, stop, event = setup

    commands.append((
        "ingestion",
        [sys.executable, str(project / "fake_ingestion.py")],
    ))
    processes.append(Mock(pid=200))
    popen.side_effect = processes

    return project, commands, processes, popen, stop, event


def test_successful_ingestion_does_not_stop_processing(
    setup, monkeypatch
):
    project, commands, _, _, stop, event = add_ingestion_job(setup)
    event.wait.side_effect = [False, False, True]

    exits = Mock(side_effect=[("ingestion", 0), None])
    monkeypatch.setattr(runner, "unexpected_exit", exits)

    code = runner.supervise_real_jobs(
        project,
        {"PROFILE": "real-test"},
        commands,
        project / "logs",
        event,
        finite_jobs=("ingestion",),
    )

    assert code == 0
    assert exits.call_count == 2

    second_monitored = exits.call_args_list[1].args[0]
    assert [
        name for name, _ in second_monitored
    ] == ["bronze", "silver", "gold"]

    assert [
        call.args[0] for call in stop.call_args_list
    ] == ["ingestion", "gold", "silver", "bronze"]


def test_failed_ingestion_stops_processing(setup, monkeypatch):
    project, commands, _, _, stop, event = add_ingestion_job(setup)
    event.wait.return_value = False

    monkeypatch.setattr(
        runner,
        "unexpected_exit",
        Mock(return_value=("ingestion", 1)),
    )

    code = runner.supervise_real_jobs(
        project,
        {"PROFILE": "real-test"},
        commands,
        project / "logs",
        event,
        finite_jobs=("ingestion",),
    )

    assert code == 1
    assert stop.call_count == 4