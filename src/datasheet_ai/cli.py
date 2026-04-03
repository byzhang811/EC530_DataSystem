"""Command line interface for the data system."""

from __future__ import annotations

import argparse
import shlex

from .errors import DataSystemError, ValidationError
from .llm_adapter import OpenAIAdapter, RuleBasedLLMAdapter
from .query_service import QueryService


HELP_TEXT = """
Commands:
  help
      Show this help message.
  tables
      List all tables in the SQLite database.
  schema
      Show all table schemas.
  load <csv_path> <table_name> [rename|overwrite|skip]
      Load CSV data into a table.
  sql <SELECT ...>
      Run a validated SQL SELECT query.
  ask <natural language request>
      Convert natural language to SQL via LLM adapter and run it.
  exit | quit
      Exit the CLI.
""".strip()


def run_cli(db_path: str, use_openai: bool = False, openai_model: str = "gpt-4.1-mini") -> None:
    llm_adapter = _build_llm_adapter(use_openai=use_openai, openai_model=openai_model)
    service = QueryService(db_path=db_path, llm_adapter=llm_adapter)
    try:
        print("DataSheet AI CLI")
        print("Type 'help' for available commands.")
        while True:
            raw = input("datasheet-ai> ").strip()
            if not raw:
                continue
            command, _, payload = raw.partition(" ")
            command = command.lower().strip()
            try:
                if command in {"exit", "quit"}:
                    print("Bye.")
                    break
                if command == "help":
                    print(HELP_TEXT)
                    continue
                if command == "tables":
                    tables = service.list_tables()
                    print("\n".join(tables) if tables else "No tables found.")
                    continue
                if command == "schema":
                    _print_schema(service)
                    continue
                if command == "load":
                    _handle_load(service, payload)
                    continue
                if command == "sql":
                    _handle_sql(service, payload)
                    continue
                if command == "ask":
                    _handle_ask(service, payload)
                    continue

                print("Unknown command. Type 'help' for usage.")
            except ValidationError as exc:
                print(f"Validation error: {' | '.join(exc.errors)}")
            except DataSystemError as exc:
                print(f"System error: {exc}")
            except Exception as exc:  # pragma: no cover - CLI fallback behavior
                print(f"Error: {exc}")
    finally:
        service.close()


def _build_llm_adapter(use_openai: bool, openai_model: str):
    if not use_openai:
        return RuleBasedLLMAdapter()
    return OpenAIAdapter(model=openai_model)


def _print_schema(service: QueryService) -> None:
    schema = service.get_database_schema()
    if not schema:
        print("No tables found.")
        return
    for table_name in sorted(schema):
        print(f"[{table_name}]")
        for column in schema[table_name].columns:
            print(f"  - {column.name}: {column.data_type}")


def _handle_load(service: QueryService, payload: str) -> None:
    args = shlex.split(payload)
    if len(args) < 2:
        raise ValueError("Usage: load <csv_path> <table_name> [rename|overwrite|skip]")
    csv_path, table_name = args[0], args[1]
    strategy = args[2] if len(args) > 2 else "rename"
    if len(args) == 2 and service.detect_schema_conflict(csv_path, table_name):
        strategy = _prompt_conflict_strategy()
    result = service.ingest_csv(csv_path, table_name, conflict_strategy=strategy)
    print(result.message)


def _prompt_conflict_strategy() -> str:
    prompt = (
        "Schema conflict detected. Choose strategy "
        "[rename/overwrite/skip] (default: rename): "
    )
    while True:
        choice = input(prompt).strip().lower() or "rename"
        if choice in {"rename", "overwrite", "skip"}:
            return choice
        print("Invalid choice. Please enter rename, overwrite, or skip.")


def _handle_sql(service: QueryService, payload: str) -> None:
    sql = payload.strip()
    if not sql:
        raise ValueError("Usage: sql <SELECT ...>")
    result = service.run_sql(sql)
    _print_query_result(result.columns, result.rows)


def _handle_ask(service: QueryService, payload: str) -> None:
    request = payload.strip()
    if not request:
        raise ValueError("Usage: ask <natural language request>")
    result = service.ask_natural_language(request)
    print(f"Generated SQL: {result.sql}")
    if result.llm_explanation:
        print(f"LLM output: {result.llm_explanation}")
    _print_query_result(result.columns, result.rows)


def _print_query_result(columns: tuple[str, ...], rows: tuple[tuple[object, ...], ...]) -> None:
    if not rows:
        print("Query executed successfully. No rows returned.")
        return

    widths = [len(column) for column in columns]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))

    header = " | ".join(column.ljust(widths[idx]) for idx, column in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)
    for row in rows:
        print(" | ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(row)))


def main() -> None:
    parser = argparse.ArgumentParser(description="DataSheet AI CLI")
    parser.add_argument("--db-path", default="data/system.db", help="Path to SQLite database file.")
    parser.add_argument(
        "--use-openai",
        action="store_true",
        help="Use OpenAI model for NL->SQL conversion (requires OPENAI_API_KEY).",
    )
    parser.add_argument(
        "--openai-model",
        default="gpt-4.1-mini",
        help="OpenAI model name used by --use-openai.",
    )
    args = parser.parse_args()
    run_cli(db_path=args.db_path, use_openai=args.use_openai, openai_model=args.openai_model)


if __name__ == "__main__":
    main()
