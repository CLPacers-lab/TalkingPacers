#!/usr/bin/env python3

import difflib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

LEGAL_LIMITATION_MESSAGE = (
    "I can calculate the payroll impact, but I do not have the CBA rule engine "
    "needed to make that legal conclusion yet."
)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def format_money(amount: int | float | None) -> str:
    if amount is None:
        return "unknown"
    return f"${amount:,.0f}"


def normalize_text(value: str) -> str:
    lowered = value.lower()
    lowered = lowered.replace("'", "")
    lowered = lowered.replace(".", "")
    lowered = lowered.replace("-", " ")
    lowered = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def parse_money_phrase(text: str) -> int | None:
    match = re.search(r"\$?\s*([0-9]+(?:\.[0-9]+)?)\s*([mk]|million|thousand)?\b", text.lower())
    if not match:
        return None

    number = float(match.group(1))
    suffix = match.group(2)

    if suffix in {"m", "million"}:
        number *= 1_000_000
    elif suffix in {"k", "thousand"}:
        number *= 1_000

    return int(round(number))


@dataclass
class StateEngineAnswer:
    kind: str
    answer: str
    sources: list[str]
    details: dict[str, Any] | None = None


class PacersStateEngine:
    def __init__(self) -> None:
        self.roster = load_json(DATA_DIR / "pacers-roster.json")
        self.contracts = load_json(DATA_DIR / "pacers-contracts.json")
        self.cap_sheet = load_json(DATA_DIR / "pacers-cap-sheet.json")
        self.cba_rules = load_json(DATA_DIR / "cba-rules.json")

        self.roster_records = self.roster["records"]
        self.contract_records = self.contracts["records"]
        self.cap_snapshot = self.cap_sheet["snapshot"]
        self.cap_metadata = self.cap_sheet["metadata"]
        self.contract_metadata = self.contracts["metadata"]
        self.cba_rule_records = self.cba_rules["records"]

        self.players = [record["player"] for record in self.roster_records]
        self.contract_by_player = {record["player"]: record for record in self.contract_records}
        self.search_aliases = self._build_search_aliases()
        self.cba_search_index = self._build_cba_search_index()

    def _build_search_aliases(self) -> dict[str, set[str]]:
        aliases: dict[str, set[str]] = {}
        for player in self.players:
            normalized_full = normalize_text(player)
            parts = normalized_full.split()
            values = {normalized_full}
            if parts:
                values.add(parts[-1])
                values.add(parts[0])
                if len(parts) >= 2:
                    values.add(" ".join(parts[-2:]))
                    values.add(parts[0] + " " + parts[-1])
            aliases[player] = values
        return aliases

    def _build_cba_search_index(self) -> list[dict[str, Any]]:
        search_rows = []
        for record in self.cba_rule_records:
            terms = [
                normalize_text(record.get("title", "")),
                normalize_text(record.get("plain_english_summary", "")),
                normalize_text(record.get("notes", "")),
            ]
            for tag in record.get("tags", []):
                terms.append(normalize_text(tag))
            for term in record.get("match_terms", []):
                terms.append(normalize_text(term))
            search_rows.append({"record": record, "terms": [term for term in terms if term]})
        return search_rows

    def answer(self, question: str) -> StateEngineAnswer:
        cleaned = question.strip()
        lowered = normalize_text(cleaned)

        if self._is_first_apron_question(lowered):
            return self._answer_first_apron_distance()

        if self._is_what_if_question(lowered):
            return self._answer_what_if(cleaned, lowered)

        if self._is_top_paid_question(lowered):
            return self._answer_highest_paid(lowered)

        if self._is_salary_lookup_question(lowered):
            return self._answer_player_salary(cleaned)

        cba_answer = self._answer_cba_rule_question(cleaned, lowered)
        if cba_answer is not None:
            return cba_answer

        if self._is_unsupported_legal_question(lowered):
            return StateEngineAnswer(
                kind="unsupported",
                answer=LEGAL_LIMITATION_MESSAGE,
                sources=self._state_sources(),
            )

        return StateEngineAnswer(
            kind="unsupported",
            answer=(
                "I can answer Pacers roster, salary, top-payroll, and simple payroll "
                "what-if questions from the current Pacers State Engine. "
                "I do not support that question yet."
            ),
            sources=self._state_sources(),
        )

    def _state_sources(self) -> list[str]:
        sources = [self.cap_metadata["source_url"]]
        for url in self.cap_metadata.get("threshold_sources", []):
            if url not in sources:
                sources.append(url)
        return sources

    def _is_cba_rule_question(self, lowered: str) -> bool:
        cba_terms = [
            "exception",
            "mle",
            "apron",
            "aggregate",
            "aggregation",
            "newly signed",
            "recently signed",
            "trade exception",
            "traded player exception",
            "traded",
            "stepien",
            "two way",
            "two-way",
            "roster limit",
            "december 15",
            "january 15",
            "hard cap",
            "hard-capped",
        ]
        return any(term in lowered for term in cba_terms) or ("rule" in lowered and "trade" in lowered)

    def _answer_cba_rule_question(self, raw_question: str, lowered: str) -> StateEngineAnswer | None:
        if not self._is_cba_rule_question(lowered):
            return None

        scored: list[tuple[int, dict[str, Any]]] = []
        for row in self.cba_search_index:
            score = 0
            for term in row["terms"]:
                if not term:
                    continue
                if term in lowered:
                    score += 6 if len(term.split()) > 1 else 2
                else:
                    token_overlap = len(set(term.split()) & set(lowered.split()))
                    score += token_overlap
            if score > 0:
                scored.append((score, row["record"]))

        scored.sort(key=lambda item: item[0], reverse=True)
        if not scored:
            return StateEngineAnswer(
                kind="lookup",
                answer="I could not find a vetted rule for that question in the current Instant GM CBA library yet.",
                sources=["2023 NBA Collective Bargaining Agreement (local page extract from data/cba_pages.json)"],
            )

        best_score, best_record = scored[0]
        related_records = [record for score, record in scored[1:3] if score >= max(4, best_score - 2)]

        if best_record["confidence"] == "needs_manual_review":
            return StateEngineAnswer(
                kind="lookup",
                answer=(
                    f"I could not find a vetted {best_record['title']} record in the current Instant GM CBA library yet. "
                    "This topic needs manual review before I should treat it as reliable."
                ),
                sources=[best_record.get("source_label", "2023 NBA Collective Bargaining Agreement (local page extract from data/cba_pages.json)")],
                details={"rule_id": best_record["rule_id"]},
            )

        article_bits = []
        if best_record.get("article"):
            article_bits.append(best_record["article"])
        if best_record.get("section"):
            article_bits.append(best_record["section"])
        if best_record.get("page") is not None:
            article_bits.append(f"page {best_record['page']}")
        citation = ", ".join(article_bits)

        lines = [f"{best_record['title']}: {best_record['plain_english_summary']}"]
        if citation:
            lines.append(f"Citation: {citation}.")
        if best_record.get("rule_text_excerpt"):
            lines.append(f"Excerpt: {best_record['rule_text_excerpt']}")

        if any(word in lowered for word in ["can we", "can i", "legally", "allowed", "is it legal"]):
            lines.append(LEGAL_LIMITATION_MESSAGE)
        else:
            lines.append("This is a general rule explanation only. I am not applying it to a full transaction yet.")

        if related_records:
            related_titles = ", ".join(record["title"] for record in related_records)
            lines.append(f"Related rules: {related_titles}.")

        sources = []
        if best_record.get("source_label"):
            sources.append(best_record["source_label"])
        for record in related_records:
            label = record.get("source_label")
            if label and label not in sources:
                sources.append(label)

        return StateEngineAnswer(
            kind="lookup",
            answer="\n".join(lines),
            sources=sources,
            details={"rule_id": best_record["rule_id"], "related_rule_ids": [record["rule_id"] for record in related_records]},
        )

    def _is_unsupported_legal_question(self, lowered: str) -> bool:
        unsupported_terms = [
            "non taxpayer mle",
            "taxpayer mle",
            "mle",
            "aggregate salaries",
            "aggregate salary",
            "can we trade",
            "trade this pick",
            "legally",
            "legal",
            "trade exception",
            "bi annual exception",
            "sign and trade",
            "stepien",
        ]
        return any(term in lowered for term in unsupported_terms)

    def _is_what_if_question(self, lowered: str) -> bool:
        return (
            lowered.startswith("what if")
            or "added a $" in lowered
            or "added $" in lowered
            or "cut $" in lowered
            or "remove " in lowered
            or "removed " in lowered
        )

    def _is_top_paid_question(self, lowered: str) -> bool:
        return (
            "highest paid" in lowered
            or "top paid" in lowered
            or "five highest paid" in lowered
            or "highest-paid" in lowered
        )

    def _is_first_apron_question(self, lowered: str) -> bool:
        return "first apron" in lowered or "apron" in lowered and "far" in lowered

    def _is_salary_lookup_question(self, lowered: str) -> bool:
        salary_terms = ["making", "salary", "paid", "earn", "earns"]
        return any(term in lowered for term in salary_terms)

    def _resolve_player(self, raw_question: str) -> tuple[str | None, list[str]]:
        normalized_question = normalize_text(raw_question)

        exact_matches = []
        for player, aliases in self.search_aliases.items():
            if any(
                alias and (f" {alias} " in f" {normalized_question} " or normalized_question == alias)
                for alias in aliases
            ):
                exact_matches.append(player)

        if len(exact_matches) == 1:
            return exact_matches[0], []

        if len(exact_matches) > 1:
            return None, sorted(exact_matches)

        token_candidates = normalized_question.split()
        fuzzy_matches: list[tuple[float, str]] = []
        for player in self.players:
            norms = self.search_aliases[player]
            best_score = max(
                difflib.SequenceMatcher(None, normalized_question, alias).ratio()
                for alias in norms
            )
            for token in token_candidates:
                best_score = max(
                    best_score,
                    max(difflib.SequenceMatcher(None, token, alias).ratio() for alias in norms),
                )
            if best_score >= 0.72:
                fuzzy_matches.append((best_score, player))

        fuzzy_matches.sort(reverse=True)
        if len(fuzzy_matches) == 1:
            return fuzzy_matches[0][1], []

        if len(fuzzy_matches) > 1:
            top_score = fuzzy_matches[0][0]
            close = [player for score, player in fuzzy_matches if top_score - score <= 0.06][:5]
            if len(close) == 1:
                return close[0], []
            return None, close

        return None, []

    def _answer_player_salary(self, question: str) -> StateEngineAnswer:
        player, options = self._resolve_player(question)
        if not player:
            if options:
                return StateEngineAnswer(
                    kind="clarification",
                    answer="I need clarification on which player you mean: " + ", ".join(options) + ".",
                    sources=[self.contract_metadata["source_url"]],
                )
            return StateEngineAnswer(
                kind="clarification",
                answer="I could not match that player to the current Pacers roster.",
                sources=[self.contract_metadata["source_url"]],
            )

        record = self.contract_by_player.get(player)
        if not record:
            return StateEngineAnswer(
                kind="unsupported",
                answer=f"I do not have a contract record for {player} in the current Pacers State Engine.",
                sources=[self.contract_metadata["source_url"]],
            )

        salary = record["salary"]
        if salary is None:
            return StateEngineAnswer(
                kind="lookup",
                answer=(
                    f"I do not have a verified current-season salary for {player} yet. "
                    "The contract row exists, but the salary is still unverified."
                ),
                sources=[record["source_url"]],
                details={"player": player},
            )

        return StateEngineAnswer(
            kind="lookup",
            answer=(
                f"{player} is making {format_money(salary)} in {record['season']}."
            ),
            sources=[record["source_url"]],
            details={"player": player, "salary": salary},
        )

    def _answer_highest_paid(self, lowered: str) -> StateEngineAnswer:
        limit = 5 if "five" in lowered else 5
        ranked = [
            record for record in self.contract_records if isinstance(record.get("salary"), int)
        ]
        ranked.sort(key=lambda record: record["salary"], reverse=True)
        top_records = ranked[:limit]

        lines = [f"Here are the {limit} highest-paid Pacers by current-season salary:"]
        for index, record in enumerate(top_records, start=1):
            lines.append(f"{index}. {record['player']}: {format_money(record['salary'])}")

        return StateEngineAnswer(
            kind="lookup",
            answer="\n".join(lines),
            sources=[record["source_url"] for record in top_records],
            details={"players": top_records},
        )

    def _answer_first_apron_distance(self) -> StateEngineAnswer:
        distance = self.cap_snapshot["distance_to_first_apron"]
        team_salary = self.cap_snapshot["team_salary"]
        first_apron = self.cap_snapshot["first_apron"]
        return StateEngineAnswer(
            kind="lookup",
            answer=(
                f"The Pacers are {format_money(distance)} below the first apron. "
                f"Current team salary: {format_money(team_salary)}. "
                f"First apron: {format_money(first_apron)}."
            ),
            sources=self._state_sources(),
            details={
                "team_salary": team_salary,
                "first_apron": first_apron,
                "distance_to_first_apron": distance,
            },
        )

    def _answer_what_if(self, raw_question: str, lowered: str) -> StateEngineAnswer:
        delta = 0
        explanation = None
        sources = self._state_sources()

        if "added" in lowered:
            amount = parse_money_phrase(raw_question)
            if amount is None:
                return StateEngineAnswer(
                    kind="clarification",
                    answer="I need the salary amount to run that add-salary scenario.",
                    sources=sources,
                )
            delta = amount
            explanation = f"Added {format_money(amount)} to team salary."
        elif "cut " in lowered:
            amount = parse_money_phrase(raw_question)
            if amount is None:
                return StateEngineAnswer(
                    kind="clarification",
                    answer="I need the salary amount to run that cut-salary scenario.",
                    sources=sources,
                )
            delta = -amount
            explanation = f"Removed {format_money(amount)} from team salary."
        elif "remove" in lowered or "removed" in lowered:
            player, options = self._resolve_player(raw_question)
            if not player:
                if options:
                    return StateEngineAnswer(
                        kind="clarification",
                        answer="I need clarification on which player you mean: " + ", ".join(options) + ".",
                        sources=sources,
                    )
                return StateEngineAnswer(
                    kind="clarification",
                    answer="I could not match that player to the current Pacers roster.",
                    sources=sources,
                )

            record = self.contract_by_player.get(player)
            salary = None if record is None else record.get("salary")
            if salary is None:
                return StateEngineAnswer(
                    kind="clarification",
                    answer=(
                        f"I found {player}, but I do not have a verified current-season salary for that player yet, "
                        "so I cannot run the payroll what-if."
                    ),
                    sources=[record["source_url"]] if record else sources,
                )
            delta = -salary
            explanation = f"Removed {player}'s salary of {format_money(salary)}."
            sources = [record["source_url"], *[url for url in sources if url != record["source_url"]]]
        else:
            return StateEngineAnswer(
                kind="unsupported",
                answer="I do not support that payroll scenario yet.",
                sources=sources,
            )

        base = self.cap_snapshot
        new_team_salary = base["team_salary"] + delta
        new_distance_to_tax = base["luxury_tax_line"] - new_team_salary
        new_distance_to_first_apron = base["first_apron"] - new_team_salary
        new_distance_to_second_apron = base["second_apron"] - new_team_salary

        answer = "\n".join(
            [
                explanation,
                f"New team salary: {format_money(new_team_salary)}",
                f"Distance to luxury tax: {format_money(new_distance_to_tax)}",
                f"Distance to first apron: {format_money(new_distance_to_first_apron)}",
                f"Distance to second apron: {format_money(new_distance_to_second_apron)}",
            ]
        )

        return StateEngineAnswer(
            kind="what_if",
            answer=answer,
            sources=sources,
            details={
                "delta": delta,
                "team_salary": new_team_salary,
                "distance_to_tax": new_distance_to_tax,
                "distance_to_first_apron": new_distance_to_first_apron,
                "distance_to_second_apron": new_distance_to_second_apron,
            },
        )
