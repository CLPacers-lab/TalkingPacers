# Instant GM Source Map

## Phase 1 Scope

This source map applies only to Phase 1.

Phase 1 scope:

- CBA rule lookup
- CBA citation links
- current Pacers roster
- current Pacers contracts / salaries
- current Pacers cap / tax / apron snapshot

## 1. CBA Rules

Best source:
- existing CBA material already present in this repository

Backup source:
- original source PDF used to generate the repository CBA data

Recommended entry mode:
- semi-manual curation of a smaller rules subset for MVP

Citation strategy:
- cite exact article / section label
- include source URL or source PDF reference

Reliability risk:
- Medium for lookup
- High if trying to convert all rules into executable logic too early

## 2. Current Pacers Roster

Best source:
- official Pacers roster page

Backup source:
- NBA.com Pacers roster page

Recommended entry mode:
- manual first

Citation strategy:
- each player row should preserve the source page URL
- answers should cite the roster source directly

Reliability risk:
- Medium
- official roster pages can lag a transaction briefly

## 3. Current Pacers Contracts / Salaries

Best source:
- Spotrac

Backup source:
- RealGM
- official Pacers / NBA transaction announcements for recent changes

Recommended entry mode:
- manual first

Citation strategy:
- each contract row should preserve the contract-page URL or salary-table URL
- recent contract events can also cite the official team or league announcement if needed

Reliability risk:
- Medium

## 4. Current Pacers Cap / Tax / Apron Snapshot

Best source:
- internal verified snapshot derived from current contracts plus published thresholds

Backup source:
- public team cap sheet
- Spotrac team salary view

Recommended entry mode:
- manually verified snapshot in Phase 1

Citation strategy:
- cite the team salary source
- cite the threshold source
- cite CBA rule text if the explanation references apron mechanics

Reliability risk:
- High
- this is the most sensitive Phase 1 fact area

## Source Governance Notes

Phase 1 should favor:

- reviewed structured entries
- small curated tables
- explicit timestamps
- explicit source URLs

Phase 1 should avoid:

- aggressive scraping
- unstable blocked sources
- silently merged facts from multiple unverified pages

## Not Yet

These are intentionally out of scope for this source map right now:

- draft pick ownership sources
- agent relationship sources
- full transaction archive sources
- trade machine sources
- full hypothetical tax simulation sources
