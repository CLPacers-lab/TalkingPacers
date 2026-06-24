# Instant GM Architecture

## Product Direction

Instant GM is now explicitly team-specific for reliability.

MVP team:
- Indiana Pacers

Core concept:
- a one-box Pacers front office copilot

The user is assumed to be asking from the Pacers' perspective when they say:
- "we"
- "our"

Example questions:
- Can we trade this pick for Player X?
- If we sign Player X for $12M, what happens to our tax/apron position?
- Which of our picks can we trade?
- Can we aggregate these salaries?
- Does Player X share an agent with anyone on our roster?
- Who becomes trade eligible next month?

## Core Principle

AI explains the result, but code and structured data determine legal and financial conclusions.

That means:
- no guessing
- no unsupported answers
- no silent reliance on model memory for cap or rule conclusions
- every answer must trace back to structured state, rule citations, or both

## Pacers State Engine

The Pacers State Engine should be the authoritative Pacers snapshot used by Instant GM.

Its job is to answer:
- what the Pacers currently are
- what the Pacers currently control
- what the Pacers are allowed to do
- what financial and legal consequences follow from a move

The engine should unify these Pacers-specific data areas:

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

## System Shape

Instant GM should be a retrieval-and-rules system, not a chat-only product.

### Request Flow

1. User submits a natural-language question.
2. Intent router identifies the front-office domain.
3. Perspective resolver interprets "we" and "our" as Pacers entities.
4. Entity resolver identifies players, picks, seasons, dates, and exceptions.
5. Query planner determines which Pacers State Engine modules are required.
6. Retrieval layer loads state records and relevant CBA passages.
7. Rules and calculation engines produce the legal or financial result.
8. Explanation layer returns a plain-English answer with citations, assumptions, and intermediate logic.
9. Refusal layer blocks unsupported answers when required data is missing.

## Major Intent Categories

- Trade legality
- Salary cap
- Luxury tax
- Draft pick ownership and protections
- Trade exceptions
- Roster status
- Contract status
- Agent relationships
- CBA rule lookup
- Transaction history

## Pacers State Engine Modules

### 1. Pacers Roster State

Purpose:
- authoritative current roster composition and roster slot status

Supports:
- Who is on our roster?
- Do we have a standard roster slot open?
- Is this player on a two-way or standard contract?

### 2. Pacers Contract State

Purpose:
- authoritative contract and salary facts by season

Supports:
- What is this player making?
- What guarantees or options matter?
- What happens if we add another contract?

### 3. Pacers Transaction and Eligibility State

Purpose:
- map acquisition history to eligibility and status changes

Supports:
- When can this player be traded?
- Who becomes trade eligible next month?
- When did we acquire or waive this player?

### 4. Pacers Asset State

Purpose:
- authoritative draft-pick ownership, protections, swaps, and encumbrances

Supports:
- Which picks can we trade?
- Is our future first encumbered?
- Does a prior obligation limit what we can offer?

### 5. Pacers Cap State

Purpose:
- current and projected salary-cap, tax, and apron status

Supports:
- What happens if we sign Player X for $12M?
- Would this move push us over the apron?
- What is our luxury-tax exposure?

### 6. Pacers Exception State

Purpose:
- track usable trade exceptions and related constraints

Supports:
- Can we use a trade exception here?
- When does a trade exception expire?

### 7. Pacers Relationship State

Purpose:
- map representation and agent connections

Supports:
- Does Player X share an agent with anyone on our roster?

### 8. CBA Rule State

Purpose:
- retrieve and cite the governing rule text

Supports:
- What rule applies here?
- Why is this legal or illegal?

## Required Engines

### Intent Router

Classifies questions into one or more domains and chooses which Pacers State Engine modules are needed.

### Perspective Resolver

Hard-codes Pacers perspective for MVP.

Examples:
- "our tax position" -> Indiana Pacers tax position
- "our 2029 first" -> Indiana Pacers controlled 2029 first-round pick state

### Entity Resolver

Resolves:
- player names
- draft picks
- seasons
- dates
- cap exceptions
- contract types

### Rules Engine

Encodes:
- trade legality restrictions
- aggregation rules
- apron restrictions
- Stepien constraints
- trade eligibility timing
- exception constraints

### Calculator Engine

Computes:
- team salary
- outgoing and incoming salary effects
- projected apron status
- projected luxury-tax outcomes

### Explanation Layer

Returns:
- conclusion
- why
- cited rule passages
- cited structured data
- assumptions and limitations

## Citation Strategy

Every answer should cite:
- a Pacers state record when a current fact is used
- a CBA rule when legality or cap mechanics are involved

Preferred answer structure:

1. Conclusion
2. Why
3. Supporting calculations or rule logic
4. Sources
5. Assumptions or missing-data warning

## Trust Model

Legal and financial answers should only be returned when:
- the relevant Pacers State Engine modules are populated
- the applicable rule logic is implemented or directly cited
- the math is deterministic

Otherwise the system should refuse cleanly.

## Not Yet

We are not building these now:

- league-wide all-team support
- player scouting or projections
- play-by-play logic
- fan-facing news chat
- strategic recommendations without structured support
- private or proprietary front-office intelligence
