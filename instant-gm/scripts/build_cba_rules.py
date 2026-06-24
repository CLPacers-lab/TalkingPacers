#!/usr/bin/env python3

import json
import re
from datetime import datetime, UTC
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
SOURCE_PATH = REPO_ROOT / "data" / "cba_pages.json"
OUTPUT_PATH = ROOT / "data" / "cba-rules.json"

SOURCE_LABEL = "2023 NBA Collective Bargaining Agreement (local page extract from data/cba_pages.json)"


def load_pages() -> dict[int, str]:
    payload = json.loads(SOURCE_PATH.read_text(encoding="utf-8"))
    return {int(item["page"]): item["text"] for item in payload}


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_excerpt(page_text: str, anchor: str, max_chars: int = 900) -> str | None:
    cleaned = clean_text(page_text)
    index = cleaned.lower().find(anchor.lower())
    if index == -1:
        return None
    excerpt = cleaned[index:index + max_chars]
    excerpt = excerpt.rsplit(" ", 1)[0]
    return excerpt


def build_rules() -> dict:
    pages = load_pages()

    specs = [
        {
            "rule_id": "bi_annual_exception",
            "title": "Bi-annual exception",
            "plain_english_summary": (
                "A team can use the bi-annual exception to sign or acquire one or more contracts "
                "with first-year salary totaling up to 3.32% of the cap. It generally lasts up to "
                "two seasons, cannot be used in consecutive cap years, and cannot be used in a year "
                "when the team already used the room exception."
            ),
            "article": "Article VII",
            "section": "Section 6(d)",
            "page": 259,
            "anchor": "A Team may use the Bi-annual Exception",
            "tags": ["exception", "bi-annual", "cap-exception", "first-apron"],
            "match_terms": ["bi annual", "bi-annual", "bae", "biannual exception"],
            "confidence": "high",
            "notes": "This rule explains the basic exception mechanics only. Using it can also trigger first-apron restrictions under the transaction restrictions table.",
        },
        {
            "rule_id": "non_taxpayer_mid_level_exception",
            "title": "Non-taxpayer mid-level exception",
            "plain_english_summary": (
                "A team can use the non-taxpayer mid-level exception to sign or acquire one or more "
                "contracts with first-year salary totaling up to 9.12% of the cap. The contract term "
                "can run up to four seasons, and the exception cannot be used in the same cap year as "
                "the room exception."
            ),
            "article": "Article VII",
            "section": "Section 6(e)",
            "page": 260,
            "anchor": "A Team may use the Non-Taxpayer Mid-Level Salary Exception",
            "tags": ["exception", "mle", "non-taxpayer-mle", "first-apron"],
            "match_terms": ["non taxpayer mle", "non-taxpayer mle", "ntmle", "mid-level exception"],
            "confidence": "high",
            "notes": "Using this exception interacts with first-apron rules and can hard-cap a team at the first apron for the rest of the cap year.",
        },
        {
            "rule_id": "taxpayer_mid_level_exception",
            "title": "Taxpayer mid-level exception",
            "plain_english_summary": (
                "A team may use the taxpayer mid-level exception only when its apron team salary "
                "immediately after using the exception exceeds the first apron. It is limited to "
                "contracts of up to two seasons and cannot be used in the same cap year as the room exception."
            ),
            "article": "Article VII",
            "section": "Section 6(f)",
            "page": 261,
            "anchor": "A Team may use the Taxpayer Mid-Level Salary Exception",
            "tags": ["exception", "mle", "taxpayer-mle", "second-apron"],
            "match_terms": ["taxpayer mle", "taxpayer mid-level", "tmle"],
            "confidence": "high",
            "notes": "The taxpayer MLE has additional apron-transaction restrictions described in Article VII Section 2(e).",
        },
        {
            "rule_id": "room_exception",
            "title": "Mid-level exception for room teams",
            "plain_english_summary": (
                "A team below the cap that is not entitled to the bi-annual, non-taxpayer MLE, or "
                "taxpayer MLE may use the room exception. It can cover up to 5.678% of the cap in "
                "first-year salary and runs up to three seasons."
            ),
            "article": "Article VII",
            "section": "Section 6(g)",
            "page": 262,
            "anchor": "Mid-Level Salary Exception for Room Teams",
            "tags": ["exception", "room-exception", "room-team", "cap-space"],
            "match_terms": ["room exception", "room mle", "room mid-level", "mid-level for room teams"],
            "confidence": "high",
            "notes": "Once a team uses the room exception, it cannot later use the bi-annual, non-taxpayer MLE, or taxpayer MLE in that same cap year.",
        },
        {
            "rule_id": "first_apron_restrictions",
            "title": "First apron transaction restrictions",
            "plain_english_summary": (
                "The CBA’s transaction restrictions table blocks certain transactions if a team would "
                "exceed the first apron immediately after the move, and after using one of those "
                "transactions the team cannot later rise above the applicable first-apron level that year."
            ),
            "article": "Article VII",
            "section": "Section 2(e)(2) and 2(e)(4)",
            "page": 214,
            "anchor": "Transaction Restrictions Table: Transaction A. Team signs or acquires a player using the Bi-annual Exception",
            "tags": ["first-apron", "apron", "restrictions", "hard-cap", "sign-and-trade"],
            "match_terms": ["first apron", "what does the first apron restrict", "first apron restrictions"],
            "confidence": "high",
            "notes": "The first-apron rows in the table include bi-annual exception use, non-taxpayer MLE use, certain sign-and-trades, certain waived-player signings, and specific traded-player-exception uses.",
        },
        {
            "rule_id": "second_apron_restrictions",
            "title": "Second apron transaction restrictions",
            "plain_english_summary": (
                "The transaction restrictions table also assigns some moves to the second apron. "
                "Those include aggregated standard traded player exceptions, cash sent in trades, "
                "certain sign-and-trade related traded player exceptions, and the taxpayer MLE."
            ),
            "article": "Article VII",
            "section": "Section 2(e)(4)",
            "page": 215,
            "anchor": "Transaction Applicable Apron Level G. Team acquires a player using a Transition Traded Player Exception",
            "tags": ["second-apron", "apron", "restrictions", "cash-in-trades", "taxpayer-mle"],
            "match_terms": ["second apron", "what does the second apron restrict", "second apron restrictions"],
            "confidence": "high",
            "notes": "This record explains the transaction categories tied to the second apron, not whether a specific proposed move is legal for the Pacers today.",
        },
        {
            "rule_id": "salary_aggregation_in_trades",
            "title": "Salary aggregation in trades",
            "plain_english_summary": (
                "The aggregated standard traded player exception allows a team to aggregate the "
                "salaries of two or more outgoing traded players in a simultaneous trade, subject "
                "to the traded-player-exception matching limits and the apron rules."
            ),
            "article": "Article VII",
            "section": "Section 6(j)(1)(ii)",
            "page": 264,
            "anchor": "Aggregated Standard Traded Player Exception",
            "tags": ["trade", "salary-aggregation", "traded-player-exception", "salary-matching"],
            "match_terms": ["aggregate salaries", "salary aggregation", "aggregate salary", "aggregate contracts"],
            "confidence": "high",
            "notes": "This is the general traded-salary aggregation rule. Separate timing restrictions apply to recently acquired players.",
        },
        {
            "rule_id": "newly_signed_player_trade_restrictions",
            "title": "Newly signed player trade restrictions",
            "plain_english_summary": (
                "Draft rookies on standard deals and players on two-way contracts generally cannot be traded "
                "for 30 days after signing. Free agents on new standard contracts generally cannot be traded "
                "until the later of three months after signing or December 15, and some larger over-the-cap "
                "re-signings are pushed to the later of three months or January 15."
            ),
            "article": "Article VII",
            "section": "Section 8(d)",
            "page": 285,
            "anchor": "No Draft Rookie who signs a Standard NBA Contract",
            "tags": ["trade", "newly-signed", "trade-restrictions", "december-15", "january-15"],
            "match_terms": ["newly signed player traded", "recently signed player trade", "december 15", "january 15"],
            "confidence": "high",
            "notes": "This record covers timing restrictions only. It does not decide whether a specific present-day trade is legal.",
        },
        {
            "rule_id": "recently_acquired_player_aggregation_restriction",
            "title": "Recently acquired player aggregation restriction",
            "plain_english_summary": (
                "If a team acquired a player via an exception, that player generally cannot be aggregated "
                "with other salaries in a trade for two months after the acquisition, subject to the CBA’s "
                "trade-deadline carveout described in the same subsection."
            ),
            "article": "Article VII",
            "section": "Section 6(j)(4)(i)",
            "page": 266,
            "anchor": "No player whose Player Contract was acquired pursuant to an Exception in the two (2) month period preceding the trade",
            "tags": ["trade", "aggregation", "recently-traded", "recently-acquired", "two-month-rule"],
            "match_terms": ["recently traded aggregation", "recently acquired aggregation", "two month trade rule"],
            "confidence": "high",
            "notes": "This is one of the most important exceptions-to-aggregation rules for trade construction.",
        },
        {
            "rule_id": "hard_cap_triggers",
            "title": "First apron hard-cap triggers",
            "plain_english_summary": (
                "Some transactions effectively hard-cap a team at the first apron for the rest of the cap year. "
                "The CBA example section shows that once a team uses a first-apron-triggering transaction, it may "
                "not later exceed the first apron for the remainder of that year."
            ),
            "article": "Article VII",
            "section": "Section 2(e)(2) and examples",
            "page": 216,
            "anchor": "As a result of such signing, pursuant to Section 2(e)(2)(i)(B) above, Team A may not, for the remainder of the 2023-24 Salary Cap Year",
            "tags": ["hard-cap", "first-apron", "non-taxpayer-mle", "bi-annual", "sign-and-trade"],
            "match_terms": ["hard cap", "hard-capped", "what hard caps a team", "hard cap triggers"],
            "confidence": "high",
            "notes": "This rule is about the triggering mechanism, not whether the Pacers currently satisfy every condition for a specific move.",
        },
        {
            "rule_id": "roster_limits",
            "title": "Roster limits",
            "plain_english_summary": (
                "During the regular season, teams generally must carry 14 or 15 players on the active and "
                "inactive lists, with only limited short-term drops to 12 or 13 players. Offseason and hardship "
                "situations are handled separately in the same article."
            ),
            "article": "Article XXIX",
            "section": "Sections 1-2",
            "page": 453,
            "anchor": "Each Team agrees to have at least twelve (12) and no more than fifteen (15) players on its Active List",
            "tags": ["roster", "roster-limits", "active-list", "inactive-list"],
            "match_terms": ["roster limit", "roster limits", "how many players", "minimum roster", "maximum roster"],
            "confidence": "high",
            "notes": "Two-way roster interactions are covered separately in the two-way rules record.",
        },
        {
            "rule_id": "two_way_contract_basics",
            "title": "Two-way contract basics",
            "plain_english_summary": (
                "Teams may carry up to three two-way players. Two-way players have active-list game limits, "
                "under-fifteen-game limits, eligibility limits based on years of service, and cannot generally "
                "appear in the postseason unless converted to a standard deal before the deadline."
            ),
            "article": "Article II and Article XXIX",
            "section": "Article II Section 11(b)-(f); Article XXIX Section 3",
            "page": 76,
            "anchor": "No Team may have on its roster at any time more than three (3) Two-Way Players",
            "tags": ["two-way", "roster", "eligibility", "postseason", "active-list"],
            "match_terms": ["two-way", "two way", "two-way contract", "two-way player"],
            "confidence": "high",
            "notes": "The excerpt is from the contract mechanics section. Postseason treatment is addressed later in Article XXIX.",
        },
        {
            "rule_id": "traded_player_exception",
            "title": "Traded player exception basics",
            "plain_english_summary": (
                "The CBA defines several traded player exceptions, including standard, aggregated standard, "
                "transition, and expanded traded player exceptions. These rules govern how much outgoing salary "
                "can be replaced and when acquired contracts can be taken in simultaneously or later."
            ),
            "article": "Article VII",
            "section": "Section 6(j)(1)",
            "page": 264,
            "anchor": "Traded Player Exception",
            "tags": ["trade", "traded-player-exception", "tpe", "salary-matching"],
            "match_terms": ["trade exception", "traded player exception", "tpe"],
            "confidence": "high",
            "notes": "This is the general traded player exception framework. Apron, aggregation, and timing restrictions are covered in related rules.",
        },
        {
            "rule_id": "stepien_rule_manual_review",
            "title": "Stepien rule / future first-round pick restrictions",
            "plain_english_summary": (
                "I do not yet have a vetted Stepien-rule record in the current local CBA extract. "
                "This topic likely needs a separate draft-pick ownership and tradeability source or "
                "a more targeted manual CBA review before it should be treated as reliable."
            ),
            "article": None,
            "section": None,
            "page": None,
            "anchor": None,
            "tags": ["draft-picks", "stepien", "future-firsts", "manual-review"],
            "match_terms": ["stepien", "future first round pick", "future first-round pick", "trade this pick"],
            "confidence": "needs_manual_review",
            "notes": "No explicit Stepien-rule passage was located in the current local extract during this implementation pass.",
        },
    ]

    records = []
    for spec in specs:
        page = spec["page"]
        excerpt = None
        if page is not None and spec["anchor"]:
            excerpt = extract_excerpt(pages[page], spec["anchor"])

        records.append(
            {
                "rule_id": spec["rule_id"],
                "title": spec["title"],
                "plain_english_summary": spec["plain_english_summary"],
                "rule_text_excerpt": excerpt,
                "article": spec["article"],
                "section": spec["section"],
                "page": page,
                "tags": spec["tags"],
                "source_label": SOURCE_LABEL,
                "confidence": spec["confidence"],
                "notes": spec["notes"],
                "match_terms": spec["match_terms"],
            }
        )

    return {
        "metadata": {
            "source_name": "Instant GM curated CBA rules library",
            "source_label": SOURCE_LABEL,
            "source_file": "data/cba_pages.json",
            "built_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rule_count": len(records),
            "notes": (
                "This is a curated rules subset intended for sourced explanation, not full legal "
                "conclusion or transaction validation."
            ),
        },
        "records": records,
    }


def main() -> int:
    payload = build_rules()
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {payload['metadata']['rule_count']} rules to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
