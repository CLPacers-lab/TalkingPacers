# Phase 1 Field Lineage

This document explains where each Phase 1 Pacers State Engine field should come from.

## pacers-roster.json

### Metadata

- `team`: fixed value for MVP, `Indiana Pacers`
- `season`: manually entered current league season
- `as_of_date`: manual snapshot date
- `updated_at`: manual or script-generated timestamp when the file is updated
- `source_name`: source label for the roster snapshot
- `source_url`: exact roster page used
- `record_count`: count of roster records

### Record fields

- `player`: source roster page
- `player_id`: optional stable ID, source if available
- `team`: fixed value, `Indiana Pacers`
- `season`: inherited from metadata or explicit row value
- `as_of_date`: inherited from metadata or explicit row value
- `position`: roster source
- `roster_type`: manually classified or source-provided
- `standard_contract`: source-provided or derived from roster classification
- `two_way`: source-provided or derived from roster classification
- `status`: roster source
- `source_url`: exact page used for the row

## pacers-contracts.json

### Metadata

- `team`: fixed value for MVP, `Indiana Pacers`
- `season`: manually entered current league season
- `as_of_date`: manual snapshot date
- `updated_at`: manual or script-generated timestamp
- `source_name`: source label for contracts snapshot
- `source_url`: exact contracts page used
- `record_count`: count of contract rows

### Record fields

- `player`: contract source
- `player_id`: optional stable ID if available
- `team`: fixed value, `Indiana Pacers`
- `season`: current season row
- `salary`: contract source
- `guaranteed_salary`: contract source
- `nonguaranteed_salary`: contract source or derived later
- `option_type`: contract source
- `option_holder`: contract source
- `option_deadline`: contract source
- `contract_year_index`: source or manually derived from contract order
- `notes`: manually normalized notes field
- `source_url`: exact source page for the contract row

## pacers-cap-sheet.json

### Metadata

- `team`: fixed value for MVP, `Indiana Pacers`
- `season`: manually entered current league season
- `as_of_date`: manual snapshot date
- `updated_at`: manual or script-generated timestamp
- `source_name`: source label for cap snapshot
- `source_url`: cap-sheet or salary-source page
- `threshold_source_url`: published threshold source

### Snapshot fields

- `team_salary`: derived from contract dataset once populated
- `salary_cap`: published threshold source
- `luxury_tax_line`: published threshold source
- `first_apron`: published threshold source
- `second_apron`: published threshold source
- `distance_to_tax`: derived from threshold minus team salary
- `distance_to_first_apron`: derived from threshold minus team salary
- `distance_to_second_apron`: derived from threshold minus team salary
- `hard_capped`: manually set or rule-derived later

## source-metadata.json

### Source entries

- `dataset`: internal dataset identifier
- `source_name`: human-readable source name
- `source_url`: exact source page
- `update_frequency`: manual update cadence
- `updated_at`: last reviewed timestamp

## Notes

Phase 1 intentionally allows manual entry and manual verification.

No player salary values should be invented.
No legal or financial conclusions should be inferred from placeholder data.
