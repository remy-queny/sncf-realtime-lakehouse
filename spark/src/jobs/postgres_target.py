from pathlib import Path


def validate_postgres_target(
    lakehouse_root: Path,
    target_schema: str,
    project_root: Path,
) -> None:
    project = project_root.resolve()
    lakehouse = lakehouse_root.resolve()

    standard_targets = {
        "public": project / "data/lakehouse",
        "sncf_real": project / "data/lakehouse_real",
    }

    if target_schema in standard_targets:
        expected_root = standard_targets[target_schema].resolve()

        if lakehouse != expected_root:
            raise ValueError(
                "Couple lakehouse/schema PostgreSQL incorrect : "
                f"{lakehouse} -> {target_schema}"
            )

        return

    if target_schema == "sncf_active_test":
        validation_root = (project / "data/validation").resolve()

        valid_test_path = (
            lakehouse.name == "lakehouse"
            and lakehouse.parent.parent == validation_root
            and lakehouse.parent.name.startswith("active-stream-")
            and lakehouse.parent.name != "active-stream-"
        )

        if not valid_test_path:
            raise ValueError(
                "sncf_active_test exige un lakehouse situé dans "
                "data/validation/active-stream-<identifiant>/lakehouse."
            )

        return

    raise ValueError(
        f"Schéma PostgreSQL non autorisé : {target_schema}"
    )