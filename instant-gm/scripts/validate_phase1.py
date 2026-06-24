#!/usr/bin/env python3

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def require_keys(obj, keys, label, errors):
    for key in keys:
      if key not in obj:
          errors.append(f"{label}: missing required key '{key}'")


def validate_metadata(payload, label, errors):
    if "metadata" not in payload or not isinstance(payload["metadata"], dict):
        errors.append(f"{label}: missing metadata object")
        return
    require_keys(
        payload["metadata"],
        ["team", "season", "as_of_date", "updated_at", "source_name", "source_url"],
        f"{label}.metadata",
        errors,
    )


def validate_roster(payload, errors):
    validate_metadata(payload, "pacers-roster.json", errors)
    records = payload.get("records")
    if not isinstance(records, list):
        errors.append("pacers-roster.json: records must be an array")
        return

    for index, record in enumerate(records):
        label = f"pacers-roster.json.records[{index}]"
        require_keys(record, ["player", "roster_type", "status", "source_url"], label, errors)


def validate_contracts(payload, errors):
    validate_metadata(payload, "pacers-contracts.json", errors)
    records = payload.get("records")
    if not isinstance(records, list):
        errors.append("pacers-contracts.json: records must be an array")
        return

    for index, record in enumerate(records):
        label = f"pacers-contracts.json.records[{index}]"
        require_keys(record, ["player", "season", "salary", "source_url"], label, errors)


def validate_cap_sheet(payload, errors):
    validate_metadata(payload, "pacers-cap-sheet.json", errors)
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        errors.append("pacers-cap-sheet.json: missing snapshot object")
        return

    require_keys(
        snapshot,
        ["salary_cap", "luxury_tax_line", "first_apron", "second_apron"],
        "pacers-cap-sheet.json.snapshot",
        errors,
    )


def validate_cba_rules(payload, errors):
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("cba-rules.json: missing metadata object")
        return

    require_keys(
        metadata,
        ["source_name", "source_label", "source_file", "built_at", "rule_count"],
        "cba-rules.json.metadata",
        errors,
    )

    records = payload.get("records")
    if not isinstance(records, list):
        errors.append("cba-rules.json: records must be an array")
        return

    for index, record in enumerate(records):
        label = f"cba-rules.json.records[{index}]"
        require_keys(
            record,
            [
                "rule_id",
                "title",
                "plain_english_summary",
                "rule_text_excerpt",
                "article",
                "section",
                "page",
                "tags",
                "confidence",
                "notes",
            ],
            label,
            errors,
        )
        if "source_url" not in record and "source_label" not in record:
            errors.append(f"{label}: missing source_url or source_label")


def validate_source_metadata(payload, errors):
    sources = payload.get("sources")
    if not isinstance(sources, list):
        errors.append("source-metadata.json: sources must be an array")
        return

    if len(sources) == 0:
        errors.append("source-metadata.json: sources array must not be empty")
        return

    for index, source in enumerate(sources):
        label = f"source-metadata.json.sources[{index}]"
        require_keys(source, ["dataset", "source_name", "source_url", "update_frequency", "updated_at"], label, errors)


def main():
    errors = []

    roster_path = DATA_DIR / "pacers-roster.json"
    contracts_path = DATA_DIR / "pacers-contracts.json"
    cap_sheet_path = DATA_DIR / "pacers-cap-sheet.json"
    cba_rules_path = DATA_DIR / "cba-rules.json"
    source_metadata_path = DATA_DIR / "source-metadata.json"

    required_files = [roster_path, contracts_path, cap_sheet_path, cba_rules_path, source_metadata_path]
    for path in required_files:
        if not path.exists():
            errors.append(f"missing required file: {path}")

    if errors:
        print("PHASE 1 VALIDATION FAILED")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    validate_roster(load_json(roster_path), errors)
    validate_contracts(load_json(contracts_path), errors)
    validate_cap_sheet(load_json(cap_sheet_path), errors)
    validate_cba_rules(load_json(cba_rules_path), errors)
    validate_source_metadata(load_json(source_metadata_path), errors)

    if errors:
        print("PHASE 1 VALIDATION FAILED")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    print("PHASE 1 VALIDATION PASSED")
    print(f"- validated: {roster_path}")
    print(f"- validated: {contracts_path}")
    print(f"- validated: {cap_sheet_path}")
    print(f"- validated: {cba_rules_path}")
    print(f"- validated: {source_metadata_path}")


if __name__ == "__main__":
    main()
