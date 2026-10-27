import importlib.util
import signal
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "active_supervision", ROOT / "scripts/run_active_simulation.py"
)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class ActiveSupervisionTests(unittest.TestCase):
    def test_default_commands_keep_bronze_silver_only(self):
        root = Path("/project")
        commands = launcher.build_job_commands(root, root / "data/validation/test")
        self.assertEqual([name for name, command in commands], ["bronze", "silver"])
        self.assertEqual(commands[0][1], [sys.executable, "/project/spark/src/jobs/stream_bronze.py"])

    def test_gold_command_has_explicit_test_directory_and_interval(self):
        root = Path("/project")
        directory = root / "data/validation/active-stream-test"
        commands = launcher.build_job_commands(root, directory, with_gold=True, gold_interval=60)
        self.assertEqual([name for name, command in commands], ["bronze", "silver", "gold"])
        self.assertEqual(commands[2][1], [
            sys.executable, "/project/scripts/run_active_gold.py", str(directory), "--interval", "60"
        ])

    def test_gold_stop_signals_python_before_its_group(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.return_value = 0
        with patch.object(launcher, "signal_group") as send_group:
            self.assertFalse(launcher.stop_process("gold", process, 30))
        process.send_signal.assert_called_once_with(signal.SIGINT)
        send_group.assert_not_called()

    def test_gold_timeout_escalates_to_group_and_is_reported(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("gold", 30), 0]
        with patch.object(launcher, "signal_group") as send_group:
            self.assertTrue(launcher.stop_process("gold", process, 30))
        process.send_signal.assert_called_once_with(signal.SIGINT)
        send_group.assert_called_once_with(process, signal.SIGTERM)

    def test_nonzero_gold_stop_is_not_reported_as_success(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "code 1"):
            launcher.stop_process("gold", process, 30)

    def test_unexpected_gold_exit_is_detected(self):
        bronze = Mock()
        bronze.poll.return_value = None
        gold = Mock()
        gold.poll.return_value = 1
        self.assertEqual(launcher.unexpected_exit([("bronze", bronze), ("gold", gold)]), ("gold", 1))


if __name__ == "__main__":
    unittest.main()