"""Static checks on the ETL scripts. No Spark needed; runs on every PR."""

import re
from pathlib import Path

import pytest

ETL = Path(__file__).resolve().parents[2] / "ETL"
SOURCES = ["suppliers", "franchises", "customers", "transactions"]
# The expression that turns a schema into the fingerprint.
FINGERPRINT = re.compile(r"hashlib\.sha256\(\s*(.*?)\.encode\(\)\s*\)\.hexdigest\(\)\[:(\d+)\]", re.S)


def _fingerprint_expression(path: Path) -> tuple[str, str]:
    match = FINGERPRINT.search(path.read_text(encoding="utf-8"))
    assert match, f"{path.name}: no schema fingerprint found"
    joined, length = match.groups()
    # Ignore which schema variable it's applied to; compare the formula.
    return re.sub(r"for f in .*\)\s*$", "for f in <schema>)", joined.strip(), flags=re.S), length


@pytest.mark.parametrize("source", SOURCES)
def test_silver_uses_the_same_fingerprint_as_bronze(source):
    """If these drift apart, silver would start a fresh checkpoint every run."""
    bronze = _fingerprint_expression(ETL / "bronze" / f"ingest_{source}.py")
    silver = _fingerprint_expression(ETL / "silver" / f"clean_{source}.py")
    assert bronze == silver


@pytest.mark.parametrize("source", SOURCES)
def test_silver_delete_step_uses_the_key_the_upsert_uses(source):
    """The snapshot's RAW_KEY must be the bronze column the upsert maps to KEY;
    otherwise every snapshot would look empty (or wrong) to the delete step."""
    src = (ETL / "silver" / f"clean_{source}.py").read_text(encoding="utf-8")
    key = re.search(r'^KEY = "(\w+)"', src, re.M).group(1)
    raw_key = re.search(r'^RAW_KEY = "(\w+)"', src, re.M).group(1)
    assert re.search(rf"^\s*{raw_key}\s+AS {key},", src, re.M), (
        f"clean_{source}.py: CLEANED doesn't map {raw_key} to {key}"
    )
