"""Database schema management and introspection."""

from __future__ import annotations

import re
import sqlite3

import pandas as pd
from pandas.api import types as ptypes

from .models import ColumnSchema, TableSchema

_IDENTIFIER_RE = re.compile(r"[^a-zA-Z0-9_]+")


def normalize_identifier(name: str) -> str:
    """Normalize a table/column name into a predictable snake_case identifier."""
    normalized = _IDENTIFIER_RE.sub("_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise ValueError("Identifier cannot be empty after normalization.")
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def quote_identifier(name: str) -> str:
    """Safely quote SQLite identifiers."""
    return '"' + name.replace('"', '""') + '"'


class SchemaManager:
    """Owns schema discovery, compatibility checks, and table creation."""

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def list_tables(self) -> list[str]:
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
        rows = self.connection.execute(query).fetchall()
        return [row[0] for row in rows]

    def table_exists(self, table_name: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    def get_table_schema(self, table_name: str) -> TableSchema | None:
        if not self.table_exists(table_name):
            return None
        pragma_sql = f"PRAGMA table_info({quote_identifier(table_name)})"
        rows = self.connection.execute(pragma_sql).fetchall()
        columns = tuple(
            ColumnSchema(name=row[1], data_type=(row[2] or "TEXT").upper()) for row in rows
        )
        return TableSchema(name=table_name, columns=columns)

    def get_database_schema(self) -> dict[str, TableSchema]:
        schema: dict[str, TableSchema] = {}
        for table_name in self.list_tables():
            table_schema = self.get_table_schema(table_name)
            if table_schema is not None:
                schema[table_name] = table_schema
        return schema

    def infer_schema_from_dataframe(self, table_name: str, dataframe: pd.DataFrame) -> TableSchema:
        columns: list[ColumnSchema] = []
        for raw_name in dataframe.columns:
            normalized_name = normalize_identifier(str(raw_name))
            inferred_type = self._infer_sqlite_type(dataframe[raw_name])
            columns.append(ColumnSchema(name=normalized_name, data_type=inferred_type))
        return TableSchema(name=table_name, columns=tuple(columns))

    def schemas_compatible(self, existing: TableSchema, inferred: TableSchema) -> bool:
        existing_map = {
            normalize_identifier(col.name): col.data_type.upper()
            for col in existing.columns
            if normalize_identifier(col.name) != "id"
        }
        inferred_map = {
            normalize_identifier(col.name): col.data_type.upper() for col in inferred.columns
        }
        return existing_map == inferred_map

    def create_table(self, table_name: str, inferred_schema: TableSchema) -> None:
        normalized_table_name = normalize_identifier(table_name)
        column_sql = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
        for column in inferred_schema.columns:
            column_sql.append(
                f"{quote_identifier(column.name)} {column.data_type.upper()}"
            )
        statement = (
            f"CREATE TABLE {quote_identifier(normalized_table_name)} "
            f"({', '.join(column_sql)})"
        )
        self.connection.execute(statement)
        self.connection.commit()

    def drop_table(self, table_name: str) -> None:
        self.connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")
        self.connection.commit()

    def get_non_conflicting_table_name(self, table_name: str) -> str:
        normalized_table_name = normalize_identifier(table_name)
        if not self.table_exists(normalized_table_name):
            return normalized_table_name
        index = 1
        while self.table_exists(f"{normalized_table_name}_{index}"):
            index += 1
        return f"{normalized_table_name}_{index}"

    @staticmethod
    def format_schema_for_prompt(schema: dict[str, TableSchema]) -> str:
        if not schema:
            return "No tables currently exist in the database."
        lines: list[str] = []
        for table_name in sorted(schema):
            columns = ", ".join(f"{col.name} ({col.data_type})" for col in schema[table_name].columns)
            lines.append(f"- {table_name}: {columns}")
        return "\n".join(lines)

    @staticmethod
    def _infer_sqlite_type(series: pd.Series) -> str:
        if ptypes.is_integer_dtype(series):
            return "INTEGER"
        if ptypes.is_bool_dtype(series):
            return "INTEGER"
        if ptypes.is_float_dtype(series):
            return "REAL"
        if ptypes.is_datetime64_any_dtype(series):
            return "TEXT"

        non_null = series.dropna()
        if non_null.empty:
            return "TEXT"
        if non_null.map(lambda value: isinstance(value, bool)).all():
            return "INTEGER"
        if non_null.map(lambda value: isinstance(value, int) and not isinstance(value, bool)).all():
            return "INTEGER"
        if non_null.map(lambda value: isinstance(value, (int, float)) and not isinstance(value, bool)).all():
            return "REAL"
        return "TEXT"
