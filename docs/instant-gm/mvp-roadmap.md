# Instant GM MVP Roadmap

## MVP Direction

Instant GM is a one-box Pacers front office copilot.

The MVP should be Pacers-specific for reliability.

The user is assumed to mean the Indiana Pacers when they ask:
- "we"
- "our"

## Phase 1 Priority

Phase 1 is:
- Build the Pacers State Engine first

Nothing else should come before that.

The Pacers State Engine is the authoritative state layer that all future legal and financial answers depend on.

## Why the Pacers State Engine Comes First

Without a reliable Pacers-specific state snapshot, Instant GM cannot safely answer:

- Can we trade this pick?
- Can we aggregate these salaries?
- What happens to our tax if we sign this player?
- Who becomes trade eligible next month?
- Does this player share an agent with anyone on our roster?

The model can explain the result, but the state engine must supply the facts.

## MVP Scope

The MVP should support a narrow but trustworthy set of Pacers front-office questions:

- CBA rule lookup
- trade eligibility dates
- draft-pick tradeability
- basic salary aggregation checks
- Pacers roster / contract / salary state lookup

## MVP Questions

The MVP should aim to answer:

- Can we trade this player right now?
- Which of our picks can we legally trade?
- Can we aggregate these salaries?
- What CBA rule applies here?
- Who becomes trade eligible next month?
- Does Player X share an agent with anyone on our roster?

## Phase Plan

## Phase 0: Planning and Data Contracts

Goals:
- finalize Pacers-only MVP boundaries
- define schemas for Pacers State Engine modules
- define citation requirements
- define refusal behavior

Deliverables:
- Pacers State Engine schemas
- intent taxonomy
- question bank
- validation rules

## Phase 1: Build the Pacers State Engine

Scope:
- current roster
- player contracts
- salaries by season
- guarantees and options
- cap holds
- dead money
- trade eligibility dates
- trade exceptions
- cap / tax / apron position
- draft pick ownership and protections
- agent relationships
- transaction history
- CBA rule links

Deliverables:
- normalized Pacers-specific state tables
- update strategy per table
- citation strategy per table
- validation harness for key Pacers scenarios

Success criteria:
- every state record has a source strategy
- key Pacers facts can be retrieved deterministically
- unsupported areas still refuse cleanly

## Phase 2: Rules and Calculation Layer

Scope:
- trade legality rules
- salary aggregation rules
- trade eligibility rules
- pick-tradeability logic
- cap / apron / tax calculators

Capabilities:
- Can we trade this pick for Player X?
- Can we aggregate these salaries?
- What happens to our tax/apron position if we sign Player X for $12M?

Success criteria:
- math is auditable
- rule decisions are cited
- deterministic scenarios pass tests

## Phase 3: Explanation Layer and Query Router

Scope:
- natural-language intent routing
- Pacers-perspective resolution for "we" and "our"
- explanation templates
- cited answer formatting

Capabilities:
- one-box natural-language experience
- structured answer explanations with citations

## Phase 4: Relationship and Workflow Features

Scope:
- agent relationship workflows
- saved scenarios
- side-by-side transaction comparisons

Capabilities:
- Does Player X share an agent with anyone on our roster?
- Compare these two move paths

## What We Are Not Building Now

Not in the current plan:

- all-30-team support
- player projections
- scouting and evaluation engine
- injury forecasting
- uncited opinionated advice
- news aggregation
- play-by-play ingestion
- fan-oriented chat experiences

## Recommended Next Decision

The next decision is not "what feature should we add?"

It is:
- Which Pacers State Engine tables are essential for the first working slice?

Recommended first slice:

1. CBA rule links
2. current roster
3. player contracts
4. salaries by season
5. transaction history
6. trade eligibility dates
7. draft pick ownership / protections

That slice is enough to start answering:
- Can we trade this player?
- Which picks can we trade?
- What rule applies here?

## Relationship to TalkingPacers

TalkingPacers should remain untouched.

Instant GM can stay in this repository temporarily as planning docs only, but it should remain logically separate because:

- the user workflows are different
- the data and validation burden are much stricter
- the deployment path will likely diverge once implementation starts
