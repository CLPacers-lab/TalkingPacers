# Instant GM Test Question Bank

## Purpose

This question bank is for validating the Phase 1 Pacers-specific MVP only.

## Phase 1 Scope

Phase 1 scope:

- CBA rule lookup
- CBA citation links
- current Pacers roster
- current Pacers contracts / salaries
- current Pacers cap / tax / apron snapshot

## Supported Test Questions

### CBA Rule Lookup

- What CBA rule applies to salary aggregation?
- What rule governs whether a newly signed player can be traded?
- What does the second apron restrict?
- What CBA rule would matter if we wanted to combine salaries in a trade?

Expected behavior:
- answer from the curated CBA rules subset
- include rule citation
- avoid pretending a full trade conclusion was made if the rule engine is not implemented

### Current Pacers Roster

- Who is currently on the Pacers roster?
- Is Player X on a standard contract or a two-way?
- Do the Pacers have an open standard roster spot?

Expected behavior:
- answer from the Pacers roster snapshot
- include roster source citation

### Current Pacers Contracts / Salaries

- What is Tyrese Haliburton making this season?
- What are the Pacers' current player salaries?
- Does Player X have an option?
- How much guaranteed money do the Pacers have committed this season?

Expected behavior:
- answer from the Pacers contracts table
- include contract source citation

### Current Pacers Cap / Tax / Apron Snapshot

- Are the Pacers currently over the luxury tax?
- How far are the Pacers from the first apron?
- What is the Pacers' current cap / tax / apron position?

Expected behavior:
- answer from the current Pacers cap snapshot
- include salary / threshold citations

## Phase 1 Refusal Questions

Phase 1 refuses:

- full trade legality
- draft pick ownership
- agent relationships
- full transaction history
- exact hypothetical tax simulations
- strategy / opinion questions

Use these as explicit refusal tests:

- Can we trade this pick for Player X?
- Can we aggregate these exact salaries in this exact deal?
- Which of our picks can we trade?
- Does Player X share an agent with anyone on our roster?
- When did we acquire this player from a full historical standpoint?
- If we sign Player X for $12M, what is our exact tax bill?
- Should we trade for Player X?

Expected behavior:
- clear refusal
- no guessing
- no unsupported legal or financial conclusion

## Validation Standards

For every supported answer:

- at least one source should be attached
- rule questions should include a rule citation
- current-state answers should include a current-state source
- the assistant should distinguish current facts from rule text

For every refused answer:

- refusal should be direct
- the assistant should not drift into speculative advice
