# Module API Specification

This file defines the main APIs (control/data/information flow) used by the system.

## CSVLoader

- `load_csv(csv_path: str, table_name: str, conflict_strategy: str = "rename") -> LoadResult`
  - Reads CSV and inserts data into SQLite (manual insert, no `df.to_sql()`).
- `detect_schema_conflict(csv_path: str, table_name: str) -> bool`
  - Returns whether existing table schema conflicts with CSV schema.

## SchemaManager

- `list_tables() -> list[str]`
- `table_exists(table_name: str) -> bool`
- `get_table_schema(table_name: str) -> TableSchema | None`
- `get_database_schema() -> dict[str, TableSchema]`
- `infer_schema_from_dataframe(table_name: str, dataframe: pd.DataFrame) -> TableSchema`
- `schemas_compatible(existing: TableSchema, inferred: TableSchema) -> bool`
- `create_table(table_name: str, inferred_schema: TableSchema) -> None`
- `drop_table(table_name: str) -> None`

## SQLValidator

- `validate(sql: str) -> ValidationResult`
  - Enforces `SELECT`-only behavior and structural safety checks.
- `assert_valid(sql: str) -> str`
  - Returns normalized SQL or raises `ValidationError`.

## LLM Adapter

- `RuleBasedLLMAdapter.generate_sql(user_request: str, schema: dict[str, TableSchema]) -> LLMOutput`
- `OpenAIAdapter.generate_sql(user_request: str, schema: dict[str, TableSchema]) -> LLMOutput`

Both adapters only generate SQL text. They do not execute SQL.

## QueryService

- `ingest_csv(csv_path: str, table_name: str, conflict_strategy: str = "rename") -> LoadResult`
- `detect_schema_conflict(csv_path: str, table_name: str) -> bool`
- `list_tables() -> list[str]`
- `get_database_schema() -> dict[str, TableSchema]`
- `run_sql(sql: str, source: str = "user_sql") -> QueryResult`
- `ask_natural_language(user_request: str) -> QueryResult`

`QueryService` is the single runtime gateway used by CLI.

## CLI

- Interactive commands:
  - `load <csv_path> <table_name> [rename|overwrite|skip]`
  - `tables`
  - `schema`
  - `sql <SELECT ...>`
  - `ask <natural language>`
  - `exit`
