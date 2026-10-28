import fcntl
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


REAL_TOPIC = "sncf.tripupdates.real.raw"
MAX_EVENTS = 20000
STATUSES = {"pending", "publishing", "published", "uncertain"}


class UncertainPublication(RuntimeError):
    pass


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, content):
    descriptor, temporary = tempfile.mkstemp(
        prefix=".publication-", suffix=".partial", dir=path.parent
    )
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            json.dump(content, file, ensure_ascii=False, indent=2, allow_nan=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def validate_metadata(metadata):
    if not isinstance(metadata, dict):
        raise ValueError("Métadonnées de lot attendues.")
    for key in ("snapshot_sha256", "jsonl_sha256"):
        value = metadata.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError(f"Empreinte invalide : {key}")
    if metadata.get("topic") != REAL_TOPIC:
        raise ValueError("Seul le topic réel est autorisé.")
    bootstrap = metadata.get("bootstrap_servers")
    if not isinstance(bootstrap, str) or not bootstrap.strip():
        raise ValueError("Adresse bootstrap absente.")
    count = metadata.get("event_count")
    if type(count) is not int or not 1 <= count <= MAX_EVENTS:
        raise ValueError("Nombre d'événements hors limites.")
    if metadata.get("selection") != "first_n":
        raise ValueError("La sélection doit être explicitement first_n.")
    filename = metadata.get("jsonl_path")
    if not isinstance(filename, str) or not Path(filename).is_absolute():
        raise ValueError("Chemin JSONL absolu requis.")
    path = Path(filename)
    if not path.is_file() or sha256(path) != metadata["jsonl_sha256"]:
        raise ValueError("Export JSONL absent ou modifié.")
    selected = []
    seen = set()
    with path.open(encoding="utf-8") as file:
        for _ in range(count):
            line = file.readline()
            if not line:
                raise ValueError("Le JSONL ne contient pas assez d'événements.")
            event = json.loads(line)
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id.strip() or event_id in seen:
                raise ValueError("event_id absent ou dupliqué dans la sélection.")
            if (event.get("source") != "sncf_gtfs_rt"
                    or event.get("event_type") != "trip_update"
                    or type(event.get("schema_version")) is not int
                    or event["schema_version"] != 1):
                raise ValueError("Observation SNCF réelle de version 1 attendue.")
            trip_id = event.get("trip_id")
            if not isinstance(trip_id, str) or not trip_id.strip():
                raise ValueError("trip_id obligatoire pour la clé Kafka.")
            selected.append(event_id)
            seen.add(event_id)
    return selected


def batch_identity(metadata):
    identity = {
        key: metadata[key]
        for key in ("snapshot_sha256", "topic", "bootstrap_servers")
    }
    return hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def validate_receipt(receipt, metadata, selected):
    if not isinstance(receipt, dict):
        raise ValueError("Reçu de publication absent.")
    if receipt.get("topic") != metadata["topic"]:
        raise ValueError("Topic du reçu incorrect.")
    if receipt.get("bootstrap_servers") != metadata["bootstrap_servers"]:
        raise ValueError("Configuration bootstrap du reçu incorrecte.")
    acknowledgements = receipt.get("acknowledgements")
    if not isinstance(acknowledgements, list) or len(acknowledgements) != len(selected):
        raise ValueError("Accusés de réception incomplets.")
    positions = set()
    for event_id, ack in zip(selected, acknowledgements):
        if not isinstance(ack, dict) or ack.get("event_id") != event_id:
            raise ValueError("Identifiants des accusés de réception incohérents.")
        if ack.get("topic") != metadata["topic"]:
            raise ValueError("Topic d'accusé de réception incorrect.")
        partition = ack.get("partition")
        offset = ack.get("offset")
        if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
            raise ValueError("Position Kafka invalide.")
        position = (partition, offset)
        if position in positions:
            raise ValueError("Position Kafka dupliquée dans le reçu.")
        positions.add(position)


class PublicationRegistry:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def locked(self):
        with (self.directory / ".publication.lock").open("a", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def path(self, batch_id):
        if not isinstance(batch_id, str) or not re.fullmatch(r"[0-9a-f]{64}", batch_id):
            raise ValueError("Identifiant de lot invalide.")
        return self.directory / f"{batch_id}.json"

    def load(self, batch_id):
        state = json.loads(self.path(batch_id).read_text(encoding="utf-8"))
        if (state.get("state_version") != 1
                or state.get("batch_id") != batch_id
                or state.get("status") not in STATUSES
                or type(state.get("attempts")) is not int
                or state["attempts"] < 0):
            raise ValueError("État de publication invalide.")
        selected = validate_metadata(state["metadata"])
        if batch_identity(state["metadata"]) != batch_id:
            raise ValueError("Identité du lot incohérente.")
        if state["status"] == "published":
            validate_receipt(state.get("receipt"), state["metadata"], selected)
        return state, selected

    def get(self, batch_id):
        return self.load(batch_id)[0]

    def prepare(self, metadata):
        metadata = clone(metadata)
        validate_metadata(metadata)
        batch_id = batch_identity(metadata)
        with self.locked():
            path = self.path(batch_id)
            if path.exists():
                state, _ = self.load(batch_id)
                if state["metadata"] != metadata:
                    raise ValueError("Lot existant : sélection ou provenance différente.")
                return state
            timestamp = now()
            state = {
                "state_version": 1,
                "batch_id": batch_id,
                "metadata": metadata,
                "status": "pending",
                "attempts": 0,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            atomic_json(path, state)
            return state

    def publish(self, batch_id, sender):
        with self.locked():
            state, selected = self.load(batch_id)
            if state["status"] == "published":
                return {"published_now": False, "state": state}
            if state["status"] in ("publishing", "uncertain"):
                raise UncertainPublication(
                    "Résultat d'envoi incertain : aucune republication automatique."
                )
            state["status"] = "publishing"
            state["attempts"] += 1
            state["updated_at"] = now()
            atomic_json(self.path(batch_id), state)
            try:
                receipt = sender(clone(state["metadata"]))
                validate_receipt(receipt, state["metadata"], selected)
                state["receipt"] = clone(receipt)
                state["status"] = "published"
                state["updated_at"] = now()
                atomic_json(self.path(batch_id), state)
            except BaseException as exception:
                state.pop("receipt", None)
                state["status"] = "uncertain"
                state["last_error"] = {
                    "type": type(exception).__name__, "message": str(exception)
                }
                state["updated_at"] = now()
                atomic_json(self.path(batch_id), state)
                raise
            return {"published_now": True, "state": state}