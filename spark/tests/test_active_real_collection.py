import fcntl
import importlib.util
import json
import signal
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "active_real_collection", ROOT / "scripts/run_active_real_collection.py"
)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
SOURCE = "https://example.invalid/feed"


@pytest.fixture
def setup(tmp_path):
    archive = tmp_path / "gtfs.zip"
    archive.write_bytes(b"gtfs-fixture")
    references = tmp_path / "references"
    references.mkdir()
    (references / "routes.csv").write_text("routes", encoding="utf-8")
    (references / "stops.csv").write_text("stops", encoding="utf-8")
    (references / "manifest.json").write_text(json.dumps({
        "source": "sncf_gtfs_static", "archive_sha256": runner.sha256(archive),
    }), encoding="utf-8")
    return tmp_path / "collection", archive, references


def normalized(new=1):
    return {
        "newly_normalized": new, "already_normalized": 0,
        "indexed_snapshots": 1, "indexed_events": 10,
    }


def run(setup, collect, normalizer, cycles=1, waiter=lambda seconds: None, source=SOURCE):
    root, archive, references = setup
    return runner.run_session(
        source, root, archive, references, 120, cycles,
        collect, lambda *args: None,
        waiter=waiter, reporter=lambda message: None, normalizer=normalizer,
    )


def create_registry_marker(root):
    (root / "registry").mkdir(exist_ok=True)
    (root / "registry/fixture.json").write_text("{}", encoding="utf-8")


def test_collects_then_normalizes_and_waits_between_cycles(setup):
    events = []
    def collect(source, root):
        events.append("collect")
        create_registry_marker(root)
    def normalize(*args):
        events.append("normalize")
        return normalized()
    result = run(setup, collect, normalize, cycles=2, waiter=lambda seconds: events.append(seconds))
    assert events == ["collect", "normalize", 120, "collect", "normalize"]
    assert result["completed_cycles"] == 2
    assert result["newly_normalized"] == 2
    assert result["kafka_enabled"] is False


def test_resumes_pending_normalization_before_new_collection(setup):
    run(setup, lambda source, root: create_registry_marker(root), lambda *args: normalized())
    events = []
    def normalize(*args):
        events.append("normalize")
        return normalized(0)
    run(setup, lambda *args: events.append("collect"), normalize)
    assert events == ["normalize", "collect", "normalize"]


def test_resume_failure_prevents_new_download(setup):
    run(setup, lambda source, root: create_registry_marker(root), lambda *args: normalized())
    collect = Mock()
    def fail(*args):
        raise RuntimeError("Normalization failed")
    with pytest.raises(RuntimeError, match="Normalization failed"):
        run(setup, collect, fail)
    collect.assert_not_called()


def test_collection_failure_prevents_normalization_and_next_cycle(setup):
    collect = Mock(side_effect=RuntimeError("Download failed"))
    normalize = Mock()
    waiter = Mock()
    with pytest.raises(RuntimeError, match="Download failed"):
        run(setup, collect, normalize, cycles=3, waiter=waiter)
    assert collect.call_count == 1
    normalize.assert_not_called()
    waiter.assert_not_called()


def test_normalization_failure_stops_next_cycle(setup):
    collect = Mock()
    normalize = Mock(side_effect=RuntimeError("Bad export"))
    waiter = Mock()
    with pytest.raises(RuntimeError, match="Bad export"):
        run(setup, collect, normalize, cycles=3, waiter=waiter)
    assert collect.call_count == 1
    waiter.assert_not_called()


def test_rejects_source_change_in_existing_session(setup):
    run(setup, Mock(), lambda *args: normalized())
    collect = Mock()
    with pytest.raises(ValueError, match="Source ou référentiel différent"):
        run(setup, collect, lambda *args: normalized(), source="https://example.invalid/other")
    collect.assert_not_called()


def test_rejects_reference_change_in_existing_session(setup):
    run(setup, Mock(), lambda *args: normalized())
    (setup[2] / "routes.csv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="Source ou référentiel différent"):
        run(setup, Mock(), lambda *args: normalized())


def test_rejects_legacy_collection_without_session_configuration(setup):
    setup[0].mkdir()
    create_registry_marker(setup[0])
    with pytest.raises(ValueError, match="Ancien dossier"):
        run(setup, Mock(), lambda *args: normalized())


def test_rejects_second_launcher_for_same_collection(setup):
    setup[0].mkdir()
    with (setup[0] / ".active-collection.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            run(setup, Mock(), lambda *args: normalized())


def test_interruption_during_pause_prevents_next_cycle(setup):
    collect = Mock()
    def stop(seconds):
        raise runner.StopRequested(signal.SIGINT)
    with pytest.raises(runner.StopRequested):
        run(setup, collect, lambda *args: normalized(), cycles=3, waiter=stop)
    assert collect.call_count == 1


def test_reference_check_rejects_unresolved_scheduled_courses():
    text = "--- Compteurs ---\ncourses_planifiees : 10\ncourses_enrichies : 9\ncourses_reference_non_resolue : 1\n"
    with pytest.raises(ValueError, match="Compatibilité GTFS incomplète"):
        runner.check_reference_output(text)


def test_reference_check_accepts_resolved_courses_and_absent_zero_counters():
    assert runner.check_reference_output(
        "--- Compteurs ---\ncourses_planifiees : 10\ncourses_enrichies : 10\n"
    ) == 10


def test_java_failure_is_not_reported_as_success():
    process = Mock()
    process.wait.return_value = 1
    process.poll.return_value = 1
    with patch.object(runner.subprocess, "Popen", return_value=process):
        with pytest.raises(subprocess.CalledProcessError):
            runner.run_java(["java", "fixture"], ROOT)
    process.send_signal.assert_not_called()


def test_interruption_signals_only_owned_java_process():
    class Process:
        def __init__(self):
            self.code = None
            self.signals = []
            self.wait_calls = 0
        def wait(self, timeout=None):
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise runner.StopRequested(signal.SIGINT)
            self.code = 130
            return self.code
        def poll(self):
            return self.code
        def send_signal(self, signum):
            self.signals.append(signum)
    process = Process()
    with patch.object(runner.subprocess, "Popen", return_value=process) as popen:
        with pytest.raises(runner.StopRequested):
            runner.run_java(["java", "fixture"], ROOT)
        assert popen.call_args.kwargs["start_new_session"] is True
    assert process.signals == [signal.SIGINT]


def test_forced_java_stop_is_a_failure_not_normal_interruption():
    class Process:
        def __init__(self):
            self.code = None
            self.wait_calls = 0
            self.signals = []
        def wait(self, timeout=None):
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise runner.StopRequested(signal.SIGINT)
            if self.wait_calls == 2:
                raise subprocess.TimeoutExpired("java", timeout)
            self.code = 143
            return self.code
        def poll(self):
            return self.code
        def send_signal(self, signum):
            self.signals.append(signum)
    process = Process()
    with patch.object(runner.subprocess, "Popen", return_value=process):
        with pytest.raises(RuntimeError, match="Arrêt Java forcé"):
            runner.run_java(["java", "fixture"], ROOT)
    assert process.signals == [signal.SIGINT, signal.SIGTERM]