"""Apply the versioned DDL scripts in SQL/ddl to one environment.

Runs as the first task of every job run, so the tables always exist and match
the repo before any ETL touches them.

How it works (a tiny Flyway-style migration runner):
  * Files are named V<number>__<description>.sql and applied in version order.
  * Each applied file is recorded in <catalog>.<prefix>ops.schema_migrations
    with a checksum, so it runs exactly once per environment.
  * Editing a file that has already been applied is an error: make a new
    V<next>__... file (e.g. ALTER TABLE ... ADD COLUMN) instead.
  * Placeholders ${catalog}, ${bronze_schema}, ${silver_schema}, ${gold_schema}
    and ${ops_schema} are filled in per environment.

The pure functions below have no Spark dependency so they are unit tested in CI
(tests/unit/test_apply_ddl.py).
"""

from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from string import Template

FILE_PATTERN = re.compile(r"^V(\d+)__([A-Za-z0-9_]+)\.sql$")
LAYERS = ("bronze", "silver", "gold", "ops", "landing")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        # Normalise line endings so Windows and Linux checkouts agree.
        return hashlib.sha256(self.sql.replace("\r\n", "\n").encode()).hexdigest()


def schema_names(schema_prefix: str) -> dict[str, str]:
    return {f"{layer}_schema": f"{schema_prefix}{layer}" for layer in LAYERS}


def discover(ddl_dir: Path) -> list[Migration]:
    migrations = []
    for path in ddl_dir.glob("*.sql"):
        match = FILE_PATTERN.match(path.name)
        if not match:
            raise ValueError(f"{path.name}: DDL files must be named V<number>__<name>.sql")
        migrations.append(
            Migration(int(match.group(1)), match.group(2), path, path.read_text(encoding="utf-8"))
        )
    migrations.sort(key=lambda m: m.version)
    versions = [m.version for m in migrations]
    duplicates = {v for v in versions if versions.count(v) > 1}
    if duplicates:
        raise ValueError(f"Duplicate DDL versions: {sorted(duplicates)}")
    return migrations


def render(sql: str, params: dict[str, str]) -> str:
    # substitute() (not safe_substitute) so a typo'd placeholder fails loudly.
    return Template(sql).substitute(params)


def split_statements(sql: str) -> list[str]:
    """Split a script on ';', ignoring '--' comments and semicolons in quotes."""
    statements, current = [], []
    quote = None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            current.append(ch)
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end == -1 else end
            continue
        elif ch == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def pending(migrations: list[Migration], applied: dict[int, str]) -> list[Migration]:
    """Return migrations still to run; fail if an applied file was edited."""
    to_run = []
    for m in migrations:
        if m.version not in applied:
            to_run.append(m)
        elif applied[m.version] != m.checksum:
            raise RuntimeError(
                f"{m.path.name} was changed after it was applied. "
                "Revert it and add a new V<next>__... migration instead."
            )
    return to_run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--ddl-dir", required=True)
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    params = {"catalog": args.catalog, **schema_names(args.schema_prefix)}
    ledger = f"`{args.catalog}`.`{params['ops_schema']}`.schema_migrations"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{args.catalog}`.`{params['ops_schema']}`")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {ledger} (
              version INT, name STRING, checksum STRING, applied_at TIMESTAMP)"""
    )
    applied = {r.version: r.checksum for r in spark.table(ledger).collect()}

    to_run = pending(discover(Path(args.ddl_dir)), applied)
    if not to_run:
        print("Schema is up to date.")
    for m in to_run:
        print(f"Applying V{m.version}__{m.name}")
        for statement in split_statements(render(m.sql, params)):
            spark.sql(statement)
        spark.sql(
            f"INSERT INTO {ledger} VALUES (:version, :name, :checksum, current_timestamp())",
            args={"version": m.version, "name": m.name, "checksum": m.checksum},
        )


if __name__ == "__main__":
    main()
