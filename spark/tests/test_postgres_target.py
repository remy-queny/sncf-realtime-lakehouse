import pytest

from jobs.postgres_target import validate_postgres_target


@pytest.mark.parametrize(
    "relative_path,schema",
    [
        ("data/lakehouse", "public"),
        ("data/lakehouse_real", "sncf_real"),
        (
            "data/validation/active-stream-example/lakehouse",
            "sncf_active_test",
        ),
    ],
)
def test_allowed_postgres_targets(
    tmp_path,
    relative_path,
    schema,
):
    validate_postgres_target(
        lakehouse_root=tmp_path / relative_path,
        target_schema=schema,
        project_root=tmp_path,
    )


@pytest.mark.parametrize(
    "relative_path,schema",
    [
        ("data/lakehouse", "sncf_real"),
        ("data/lakehouse_real", "public"),
        ("data/lakehouse", "sncf_active_test"),
        ("data/lakehouse_real", "sncf_active_test"),
        (
            "data/validation/active-stream-example/lakehouse",
            "public",
        ),
        (
            "data/validation/active-stream-example/lakehouse",
            "sncf_real",
        ),
        (
            "data/validation/other/lakehouse",
            "sncf_active_test",
        ),
        (
            "data/validation/active-stream-/lakehouse",
            "sncf_active_test",
        ),
        (
            "data/validation/active-stream-example/other",
            "sncf_active_test",
        ),
        (
            "data/validation/active-stream-example/nested/lakehouse",
            "sncf_active_test",
        ),
        ("data/lakehouse", "unknown"),
    ],
)
def test_incorrect_postgres_targets_are_rejected(
    tmp_path,
    relative_path,
    schema,
):
    with pytest.raises(ValueError):
        validate_postgres_target(
            lakehouse_root=tmp_path / relative_path,
            target_schema=schema,
            project_root=tmp_path,
        )


def test_external_directory_is_rejected(tmp_path):
    project = tmp_path / "project"
    external = tmp_path / "external/active-stream-example/lakehouse"

    with pytest.raises(ValueError):
        validate_postgres_target(
            lakehouse_root=external,
            target_schema="sncf_active_test",
            project_root=project,
        )


def test_symlink_to_real_lakehouse_is_rejected(tmp_path):
    real = tmp_path / "data/lakehouse_real"
    real.mkdir(parents=True)

    link = (
        tmp_path
        / "data/validation/active-stream-example/lakehouse"
    )
    link.parent.mkdir(parents=True)
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(ValueError):
        validate_postgres_target(
            lakehouse_root=link,
            target_schema="sncf_active_test",
            project_root=tmp_path,
        )


def test_symlink_to_external_test_directory_is_rejected(tmp_path):
    project = tmp_path / "project"
    external = (
        tmp_path
        / "external/active-stream-example/lakehouse"
    )
    external.mkdir(parents=True)

    link = project / "data/validation/active-stream-example"
    link.parent.mkdir(parents=True)
    link.symlink_to(
        external.parent,
        target_is_directory=True,
    )

    with pytest.raises(ValueError):
        validate_postgres_target(
            lakehouse_root=link / "lakehouse",
            target_schema="sncf_active_test",
            project_root=project,
        )
        