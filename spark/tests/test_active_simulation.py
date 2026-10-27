import importlib.util
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "active_simulation", ROOT / "scripts/run_active_simulation.py"
)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ActiveSimulationTests(unittest.TestCase):
    def test_environment_overrides_real_profile(self):
        env = launcher.build_environment(
            Path("/project"), Path("/project/data/validation/test"), 10,
            {"KAFKA_TRIP_UPDATES_TOPIC": "real", "POSTGRES_TARGET_SCHEMA": "sncf_real"},
        )
        self.assertEqual(env["KAFKA_TRIP_UPDATES_TOPIC"], "sncf.trip_updates.raw")
        self.assertEqual(env["SPARK_MASTER"], "local[2]")
        self.assertEqual(env["STREAMING_MODE"], "active")
        self.assertEqual(env["STREAMING_INTERVAL_SECONDS"], "10")
        self.assertNotIn("POSTGRES_TARGET_SCHEMA", env)

    def test_positive_interval(self):
        self.assertEqual(launcher.positive_integer("10"), 10)
        for value in ("0", "-1", "abc"):
            with self.subTest(value=value), self.assertRaises((ValueError, launcher.argparse.ArgumentTypeError)):
                launcher.positive_integer(value)

    def test_rejects_production_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path in (root / "data/lakehouse", root / "data/lakehouse_real", root / "data/validation"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    launcher.validate_test_directory(root, path)

    def test_rejects_unprepared_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                launcher.validate_test_directory(Path(directory), "data/validation/test")

    def test_accepts_existing_test_layout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            test = root / "data/validation/test"
            for relative in ("lakehouse/bronze/trip_updates/_delta_log", "checkpoints/bronze", "checkpoints/silver_trip_delays", "checkpoints/silver_rejected_events"):
                (test / relative).mkdir(parents=True)
            reference = root / "spark/resources/simulation"
            reference.mkdir(parents=True)
            for name in ("routes.csv", "stops.csv"):
                (reference / name).write_text("test")
            self.assertEqual(launcher.validate_test_directory(root, test), test.resolve())

    def test_running_processes_have_no_exit(self):
        process = Mock()
        process.poll.return_value = None
        self.assertIsNone(launcher.unexpected_exit([("bronze", process)]))

    def test_even_zero_exit_is_unexpected_in_active_mode(self):
        process = Mock()
        process.poll.return_value = 0
        self.assertEqual(launcher.unexpected_exit([("silver", process)]), ("silver", 0))

    def test_normal_stop(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        with patch.object(launcher, "signal_group") as send:
            self.assertFalse(launcher.stop_process("silver", process, 30))
        send.assert_called_once_with(process, signal.SIGINT)
        process.wait.assert_called_once_with(timeout=30)

    def test_stop_escalation_is_reported(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("test", 30), 0]
        with patch.object(launcher, "signal_group") as send:
            self.assertTrue(launcher.stop_process("silver", process, 30))
        self.assertEqual([call.args[1] for call in send.call_args_list], [signal.SIGINT, signal.SIGTERM])

    def test_exited_process_is_not_signalled(self):
        process = Mock()
        process.poll.return_value = 0
        with patch.object(launcher, "signal_group") as send:
            self.assertFalse(launcher.stop_process("bronze", process, 30))
        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
