# Instant GM Phase 1 Implementation Plan

## Phase 1 Goal

Phase 1 is a tightly scoped Pacers-specific front office assistant.

It should answer basic rules and current-team-state questions with citations, but it should not attempt full trade machine or full cap-engine conclusions yet.

## Phase 1 Scope

Phase 1 scope:

- CBA rule lookup
- CBA citation links
- current Pacers roster
- current Pacers contracts / salaries
- current Pacers cap / tax / apron snapshot

## Product Framing

Instant GM is a one-box Pacers front office copilot.

The user is assumed to be asking from the Pacers' perspective when they say:

- "we"
- "our"

The model may explain the answer, but structured data and code should determine any legal or financial conclusion.

## Planned Files

These are planning targets only for Phase 1. They are not being created yet outside docs.

Future implementation targets:

- `instant-gm/data/cba-rules.json`
- `instant-gm/data/pacers-roster.json`
- `instant-gm/data/pacers-contracts.json`
- `instant-gm/data/pacers-cap-sheet.json`
- `instant-gm/data/source-metadata.json`
- `instant-gm/data/last-updated.json`

## Planned Data Shapes

### CBA Rules

Purpose:
- answer rule lookup questions
- attach rule citations to explanations

Core fields:

- `rule_id`
- `article`
- `section`
- `title`
- `text`
- `summary`
- `tags`
- `source_url`
- `citation_label`

### Current Pacers Roster

Purpose:
- answer current roster-state questions

Core fields:

- `team`
- `as_of_date`
- `player`
- `player_id`
- `roster_type`
- `standard_contract`
- `two_way`
- `status`
- `position`
- `source_url`

### Current Pacers Contracts / Salaries

Purpose:
- answer current salary and contract-state questions

Core fields:

- `team`
- `player`
- `player_id`
- `season`
- `salary`
- `guaranteed_salary`
- `nonguaranteed_salary`
- `option_type`
- `option_holder`
- `option_deadline`
- `contract_year_index`
- `notes`
- `source_url`

### Current Pacers Cap / Tax / Apron Snapshot

Purpose:
- answer current team financial-position questions

Core fields:

- `team`
- `season`
- `team_salary`
- `salary_cap`
- `luxury_tax_line`
- `first_apron`
- `second_apron`
- `distance_to_tax`
- `distance_to_first_apron`
- `distance_to_second_apron`
- `hard_capped`
- `source_url`
- `threshold_source_url`

## Recommended Build Order

1. Curated CBA rules subset
2. Current Pacers roster table
3. Current Pacers contracts / salaries table
4. Current Pacers cap / tax / apron snapshot
5. Query routing limited to those four data areas
6. Refusal layer for unsupported question classes

## Manual vs Automated Entry

Phase 1 should bias toward reviewed, structured data instead of broad automation.

Recommended approach:

- CBA rules: semi-manual curation from existing repo CBA material
- current Pacers roster: manual first
- current Pacers contracts / salaries: manual first
- current Pacers cap / tax / apron snapshot: manually verified snapshot derived from the contract table and threshold inputs

Reason:
- lower implementation risk
- clearer validation
- safer citation trail

## Citation Strategy

Every answer should cite:

- the fact source when the answer depends on current team state
- the rule source when the answer depends on CBA text

Preferred answer structure:

1. Conclusion
2. Why
3. Supporting facts or rule logic
4. Sources
5. Assumptions or limits

## Phase 1 Supports

Examples of supported questions:

- What CBA rule applies to salary aggregation?
- What does the second apron restrict?
- Who is currently on the Pacers roster?
- Is Player X on a standard contract or a two-way?
- What is Tyrese Haliburton making this season?
- What are the Pacers' current player salaries?
- Are the Pacers currently over the luxury tax?
- How far are the Pacers from the first apron?

## Phase 1 Refuses

Phase 1 refuses:

- full trade legality
- draft pick ownership
- agent relationships
- full transaction history
- exact hypothetical tax simulations
- strategy / opinion questions

More specifically, Phase 1 should refuse:

- Can we trade this pick for Player X?
- Can we aggregate these exact salaries in this exact deal?
- Which of our picks can we trade?
- Does Player X share an agent with anyone on our roster?
- When did we acquire this player across full historical transactions?
- If we sign Player X for $12M, what is our exact tax bill?
- Should we trade for Player X?

## Refusal Standard

If a question requires a dataset or calculator outside Phase 1 scope, the assistant should refuse clearly instead of guessing.

## Success Criteria

Phase 1 is successful if:

- CBA rule answers are cited
- current Pacers state answers are cited
- unsupported trade-machine questions refuse cleanly
- no answer relies on unstated model memory for legal or financial conclusions
