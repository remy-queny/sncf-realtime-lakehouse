import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path, PurePosixPath
from zipfile import ZipFile


def read_table(archive, filename, required_columns):
    matches = [
        name
        for name in archive.namelist()
        if PurePosixPath(name).name == filename
        and not name.endswith("/")
    ]

    if len(matches) != 1:
        raise ValueError(f"Fichier absent ou ambigu : {filename}")

    with archive.open(matches[0]) as raw:
        with TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text:
            reader = csv.DictReader(text)
            headers = reader.fieldnames or []

            if len(headers) != len(set(headers)):
                raise ValueError(f"En-têtes dupliqués : {filename}")

            missing = set(required_columns) - set(headers)
            if missing:
                raise ValueError(
                    f"{filename} : colonnes absentes : {sorted(missing)}"
                )

            rows = list(reader)

    if not rows:
        raise ValueError(f"Table vide : {filename}")

    return rows


def prepare_rows(rows, id_column, name_columns, output_name):
    result = []
    seen = set()

    for row in rows:
        identifier = row[id_column]

        if identifier is None or not identifier.strip():
            raise ValueError(f"Identifiant vide : {id_column}")

        if identifier in seen:
            raise ValueError(f"Identifiant dupliqué : {identifier}")

        name = next(
            (
                value.strip()
                for column in name_columns
                if (value := row.get(column))
                and value.strip()
            ),
            None,
        )

        if name is None:
            raise ValueError(f"Nom absent pour : {identifier}")

        seen.add(identifier)
        result.append({
            id_column: identifier,
            output_name: name,
        })

    return sorted(result, key=lambda row: row[id_column])


def write_csv(path, columns, rows):
    with path.open("x", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Préparer les référentiels SNCF réels pour Silver."
    )
    parser.add_argument("archive", type=Path)
    parser.add_argument("output_directory", type=Path)
    args = parser.parse_args()

    if args.output_directory.exists():
        raise SystemExit(
            f"Le dossier existe déjà : {args.output_directory}. "
            "Choisir un nouveau dossier, sans écraser le référentiel."
        )

    with ZipFile(args.archive) as archive:
        routes = prepare_rows(
            read_table(
                archive,
                "routes.txt",
                {"route_id", "route_long_name", "route_short_name"},
            ),
            "route_id",
            ("route_long_name", "route_short_name"),
            "line_name",
        )

        stops = prepare_rows(
            read_table(
                archive,
                "stops.txt",
                {"stop_id", "stop_name"},
            ),
            "stop_id",
            ("stop_name",),
            "stop_name",
        )

    with args.archive.open("rb") as file:
        archive_sha256 = hashlib.file_digest(file, "sha256").hexdigest()

    args.output_directory.mkdir(parents=True, exist_ok=False)

    write_csv(
        args.output_directory / "routes.csv",
        ("route_id", "line_name"),
        routes,
    )
    write_csv(
        args.output_directory / "stops.csv",
        ("stop_id", "stop_name"),
        stops,
    )

    manifest = {
        "source": "sncf_gtfs_static",
        "archive_filename": args.archive.name,
        "archive_sha256": archive_sha256,
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "route_count": len(routes),
        "stop_count": len(stops),
        "line_name_rule": "route_long_name else route_short_name",
    }

    with (args.output_directory / "manifest.json").open(
        "x", encoding="utf-8"
    ) as file:
        json.dump(manifest, file, ensure_ascii=False, indent=2)

    print(f"Lignes préparées : {len(routes)}")
    print(f"Arrêts préparés : {len(stops)}")
    print(f"SHA-256 de l'archive : {archive_sha256}")
    print(f"Référentiels : {args.output_directory.resolve()}")
    print("Préparation terminée.")


if __name__ == "__main__":
    main()