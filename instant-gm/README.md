# Instant GM

Instant GM is a separate product scaffold inside this repository.

This folder is intentionally isolated from the live TalkingPacers app.

## Phase 1 Scope

Phase 1 covers only:

- CBA rule lookup planning
- current Pacers roster dataset
- current Pacers contracts / salaries dataset
- current Pacers cap / tax / apron snapshot dataset

Phase 1 does not yet include:

- chatbot routing
- UI work
- trade legality engine
- draft pick ownership
- agent relationships
- full transaction history
- exact hypothetical tax simulations

## Current State

This scaffold includes:

- empty JSON dataset templates
- source metadata template
- field-lineage documentation
- a validation script for Phase 1 data structure

## Validation

Run:

```bash
python3 instant-gm/scripts/validate_phase1.py
```

The validator checks schema presence and required fields, but it does not verify business correctness yet.
