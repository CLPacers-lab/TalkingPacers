#!/usr/bin/env python3

import argparse
import sys

from query_state_engine import PacersStateEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Instant GM Pacers State Engine CLI")
    subparsers = parser.add_subparsers(dest="command")

    ask_parser = subparsers.add_parser("ask", help="Ask a payroll or roster question")
    ask_parser.add_argument("question", help="Natural-language question to answer")

    return parser


def print_answer(answer) -> None:
    print(answer.answer)
    if answer.sources:
        print("\nSources:")
        for source in answer.sources:
            print(f"- {source}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command != "ask":
        parser.print_help()
        return 1

    engine = PacersStateEngine()
    answer = engine.answer(args.question)
    print_answer(answer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
