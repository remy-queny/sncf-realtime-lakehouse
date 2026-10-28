import ast
import re
import shlex
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_REAL_TOPIC = "sncf.trip_updates.real.raw"


def python_constant(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    values = [
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    ]
    assert len(values) == 1, f"Une déclaration unique de {name} est attendue dans {path}"
    return values[0]


def shell_value(path, name):
    pattern = re.compile(rf"^(?:export\s+)?{re.escape(name)}=")
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if pattern.match(stripped):
            assignments = [
                token for token in shlex.split(stripped, comments=True)
                if token.startswith(name + "=")
            ]
            values.extend(token.split("=", 1)[1] for token in assignments)
    assert len(values) == 1, f"Une affectation unique de {name} est attendue dans {path}"
    return values[0]


def test_registry_uses_historical_real_topic():
    assert python_constant(
        ROOT / "scripts/publication_state.py", "REAL_TOPIC"
    ) == EXPECTED_REAL_TOPIC


def test_java_producer_uses_historical_real_topic():
    path = ROOT / "producer-java/src/main/java/fr/remyqueny/sncf/producer/RealJsonlProducer.java"
    values = re.findall(
        r'\bString\s+TOPIC\s*=\s*"([^"\r\n]+)"\s*;',
        path.read_text(encoding="utf-8"),
    )
    assert values == [EXPECTED_REAL_TOPIC]


def test_topic_creation_script_uses_historical_real_topic():
    assert shell_value(
        ROOT / "scripts/create_real_topic.sh", "TOPIC"
    ) == EXPECTED_REAL_TOPIC


def test_real_pipeline_uses_historical_real_topic():
    assert shell_value(
        ROOT / "scripts/run_real_pipeline.sh", "KAFKA_TRIP_UPDATES_TOPIC"
    ) == EXPECTED_REAL_TOPIC