# Pacers State Engine Data Model

## Overview

The Pacers State Engine is the authoritative Indiana Pacers snapshot for Instant GM.

The MVP assumes the user is asking from the Pacers' perspective.

Each data area below describes:
1. required fields
2. best source
3. backup source
4. update frequency
5. reliability risk
6. citation strategy
7. example questions supported

## 1. Current Roster

### Required fields

- `team`
- `as_of_date`
- `player`
- `player_id`
- `roster_type`
- `standard_contract`
- `two_way`
- `exhibit_10`
- `training_camp_deal`
- `waived`
- `inactive_reason`
- `source_url`
- `updated_at`

### Best source

- Official Pacers roster and transaction pages

### Backup source

- Spotrac roster pages
- NBA.com team roster pages

### Update frequency

- Daily during active roster periods
- Immediately after transactions when possible

### Reliability risk

- Medium
- roster pages can lag behind transaction announcements

### Citation strategy

- cite the current Pacers roster page or the corresponding transaction page

### Example questions supported

- Do we have an open roster spot?
- Is this player on a two-way?
- Who is currently on our standard roster?

## 2. Player Contracts

### Required fields

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
- `bonus_notes`
- `trade_kicker`
- `contract_year_index`
- `source_url`
- `updated_at`

### Best source

- Spotrac contract pages

### Backup source

- RealGM contract pages
- official signing/extension press releases for verification

### Update frequency

- Weekly baseline
- immediately after signings, options, extensions, or waives

### Reliability risk

- Medium
- public contract sites are strong but not official CBA ledgers

### Citation strategy

- cite player contract page for current salary facts
- cite official press release when confirming a new contract event

### Example questions supported

- What is this player making this season?
- How much guaranteed money do we have committed next year?
- What contract year is this player in?

## 3. Salaries by Season

### Required fields

- `team`
- `season`
- `player`
- `player_id`
- `salary`
- `guaranteed_salary`
- `cap_hit`
- `source_url`

### Best source

- Spotrac salary tables

### Backup source

- RealGM salary tables

### Update frequency

- Weekly baseline
- after all contract-affecting moves

### Reliability risk

- Medium

### Citation strategy

- cite season salary table URL

### Example questions supported

- What is our team salary this season?
- How much salary do we have committed in 2026-27?

## 4. Guarantees and Options

### Required fields

- `player`
- `season`
- `guarantee_date`
- `guaranteed_salary`
- `nonguaranteed_salary`
- `option_type`
- `option_holder`
- `option_deadline`
- `source_url`

### Best source

- Spotrac contract detail pages

### Backup source

- official contract announcements
- RealGM contract detail pages

### Update frequency

- on contract events and option-deadline cycles

### Reliability risk

- Medium

### Citation strategy

- cite contract detail page
- optionally pair with official release for major options/extensions

### Example questions supported

- Does this player have a team option?
- When do we need to decide on this option?
- How much of this salary is guaranteed?

## 5. Cap Holds

### Required fields

- `player`
- `season`
- `cap_hold_type`
- `cap_hold_amount`
- `renounced`
- `source_url`

### Best source

- public cap sheets maintained from CBA rules and salary data

### Backup source

- Spotrac if surfaced clearly
- manual state table built from transactions and contract status

### Update frequency

- after free agency, renouncements, and qualifying-offer events

### Reliability risk

- High
- cap holds are easy to misstate without a disciplined rules layer

### Citation strategy

- cite cap-sheet page plus CBA rule reference for calculation logic

### Example questions supported

- What cap holds are on our books?
- What happens if we renounce this player?

## 6. Dead Money

### Required fields

- `player`
- `season`
- `dead_money_amount`
- `origin`
- `stretch_applied`
- `source_url`

### Best source

- Spotrac dead-cap/dead-money views where available

### Backup source

- transaction history plus internal calculation logic

### Update frequency

- after waives, stretches, or guarantees becoming dead salary

### Reliability risk

- Medium to High

### Citation strategy

- cite public cap sheet or dead-money page
- cite transaction if the dead money derives from a recent move

### Example questions supported

- How much dead money do we have this season?
- What is still on our cap from waived Player X?

## 7. Trade Eligibility Dates

### Required fields

- `player`
- `as_of_date`
- `trade_eligible`
- `eligibility_date`
- `reason`
- `source_transaction_url`
- `source_rule_url`

### Best source

- internal derived table built from transactions plus CBA rules

### Backup source

- official transaction logs
- public cap/trade eligibility trackers

### Update frequency

- after every signing, extension, or acquisition affecting eligibility

### Reliability risk

- High
- this requires correct rule encoding

### Citation strategy

- cite both the triggering transaction and the governing CBA rule

### Example questions supported

- Who becomes trade eligible next month?
- Can we trade this player today?

## 8. Trade Exceptions

### Required fields

- `team`
- `exception_type`
- `amount`
- `created_date`
- `expiration_date`
- `origin_transaction`
- `source_url`

### Best source

- public cap sheets

### Backup source

- Spotrac if surfaced clearly
- manual derived table from transactions

### Update frequency

- after each transaction creating or consuming an exception

### Reliability risk

- High
- exception logic is easy to misapply

### Citation strategy

- cite cap-sheet source plus origin transaction when possible

### Example questions supported

- Do we have a trade exception we can use?
- When does this exception expire?

## 9. Cap / Tax / Apron Position

### Required fields

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
- `rule_source_url`

### Best source

- internal calculated state using salary data plus thresholds

### Backup source

- public cap sheets
- Spotrac team salary views

### Update frequency

- daily during offseason and transaction periods
- after every roster-affecting move

### Reliability risk

- High
- requires disciplined salary-state maintenance

### Citation strategy

- cite structured team salary source and threshold source
- cite CBA rule passages where apron logic matters

### Example questions supported

- If we sign Player X for $12M, what happens to our tax position?
- Would this trade push us over the first apron?

## 10. Draft Pick Ownership / Protections

### Required fields

- `team`
- `pick_year`
- `round`
- `current_owner`
- `original_owner`
- `protection_text`
- `swap_rights`
- `encumbered`
- `conveyance_order`
- `source_url`

### Best source

- verified public trade-asset source with clear protection chains

### Backup source

- official trade press releases
- manually maintained Pacers pick ledger

### Update frequency

- after every trade affecting draft assets

### Reliability risk

- Very High
- pick protections and conveyance chains are easy to get wrong

### Citation strategy

- cite pick ledger source
- cite official transaction announcements for major trades

### Example questions supported

- Which of our picks can we trade?
- Is our 2029 first available?
- Does this obligation block a future pick trade?

## 11. Agent Relationships

### Required fields

- `player`
- `agent`
- `agency`
- `effective_date`
- `source_url`

### Best source

- public representation database or contract site with agent fields

### Backup source

- player profile pages and agency announcements

### Update frequency

- monthly baseline
- on known representation changes

### Reliability risk

- Medium

### Citation strategy

- cite representation source per player

### Example questions supported

- Does Player X share an agent with anyone on our roster?
- Which Pacers players are represented by Agency Y?

## 12. Transaction History

### Required fields

- `date`
- `team`
- `player`
- `transaction_type`
- `description`
- `players_in`
- `players_out`
- `counterpart_team`
- `source_url`
- `raw_source`

### Best source

- official Pacers / NBA transaction and press-release history where practical

### Backup source

- structured public transaction archive

### Update frequency

- immediately after transaction events

### Reliability risk

- Medium
- completeness may vary by source

### Citation strategy

- cite official transaction post when available
- otherwise cite normalized archive source

### Example questions supported

- When did we acquire Haliburton?
- When did we sign T.J. McConnell?
- Who did we waive this season?

## 13. CBA Rule Links

### Required fields

- `rule_id`
- `article`
- `section`
- `title`
- `text`
- `tags`
- `source_url`
- `citation_label`

### Best source

- the CBA material already in this repository, normalized and section-tagged

### Backup source

- source PDF reference

### Update frequency

- only when CBA source text or rule indexing changes

### Reliability risk

- Medium for lookup
- High when turning text into executable rule logic

### Citation strategy

- cite exact rule section and source page reference

### Example questions supported

- What rule applies here?
- Why can't we aggregate these salaries?
- What CBA rule controls this trade-eligibility date?

## Entity Standards

All Pacers State Engine datasets should standardize:

- `team_id`
- `player_id`
- `season`
- `as_of_date`
- `source_url`
- `source_name`
- `updated_at`

## Not Yet

We are not planning these now:

- play-by-play
- projections
- scouting grades
- medical data
- private front-office notes
- league-wide state for all 30 teams
