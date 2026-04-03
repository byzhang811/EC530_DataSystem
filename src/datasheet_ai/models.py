"""Shared data models."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    data_type: str


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple[ColumnSchema, ...]


@dataclass(frozen=True)
class LoadResult:
    table_name: str
    rows_inserted: int
    created_new_table: bool
    message: str = ""


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: tuple[str, ...] = field(default_factory=tuple)
    normalized_sql: str = ""


@dataclass(frozen=True)
class LLMOutput:
    sql: str
    explanation: str = ""


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    sql: str
    source: str = "user_sql"
    llm_explanation: str = ""
