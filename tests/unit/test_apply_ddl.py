"""Unit tests for the DDL runner. No Spark needed; runs on every PR."""

from pathlib import Path

import pytest

from apply_ddl import Migration, discover, pending, render, schema_names, split_statements

REPO_DDL_DIR = Path(__file__).resolve().parents[2] / "SQL" / "ddl"


def test_repo_ddl_files_are_valid_and_fully_templated():
    migrations = discover(REPO_DDL_DIR)
    assert migrations, "no DDL files found"
    assert [m.version for m in migrations] == sorted(m.version for m in migrations)
    params = {"catalog": "c", **schema_names("dev_")}
    for m in migrations:
        statements = split_statements(render(m.sql, params))  # raises on unknown ${...}
        assert statements, f"{m.path.name} has no statements"


def test_schema_names_use_prefix():
    assert schema_names("dev_") == {
        "bronze_schema": "dev_bronze",
        "silver_schema": "dev_silver",
        "gold_schema": "dev_gold",
        "ops_schema": "dev_ops",
        "landing_schema": "dev_landing"
    }


def test_render_fails_on_unknown_placeholder():
    with pytest.raises(KeyError):
        render("CREATE TABLE ${nope}.t (a INT)", {"catalog": "c"})


def test_split_statements_ignores_comments_and_quoted_semicolons():
    sql = """
    -- a comment; with a semicolon
    CREATE TABLE t (a STRING COMMENT 'x;y');
    INSERT INTO t VALUES ('a;b');  -- trailing; comment
    """
    assert split_statements(sql) == [
        "CREATE TABLE t (a STRING COMMENT 'x;y')",
        "INSERT INTO t VALUES ('a;b')",
    ]


def test_discover_rejects_bad_names(tmp_path):
    (tmp_path / "create_tables.sql").write_text("SELECT 1")
    with pytest.raises(ValueError, match="V<number>__<name>.sql"):
        discover(tmp_path)


def test_discover_rejects_duplicate_versions(tmp_path):
    (tmp_path / "V001__a.sql").write_text("SELECT 1")
    (tmp_path / "V1__b.sql").write_text("SELECT 2")
    with pytest.raises(ValueError, match="Duplicate"):
        discover(tmp_path)


def _migration(version, sql="SELECT 1"):
    return Migration(version, f"m{version}", Path(f"V{version}__m{version}.sql"), sql)


def test_pending_skips_applied_and_returns_new():
    m1, m2 = _migration(1), _migration(2)
    assert pending([m1, m2], {1: m1.checksum}) == [m2]


def test_pending_fails_if_applied_file_was_edited():
    m1 = _migration(1, "SELECT 1")
    edited = _migration(1, "SELECT 2")
    with pytest.raises(RuntimeError, match="changed after it was applied"):
        pending([edited], {1: m1.checksum})


def test_checksum_ignores_line_endings():
    assert _migration(1, "a\r\nb").checksum == _migration(1, "a\nb").checksum
