"""Query validation layer that guards database execution."""

from __future__ import annotations

import re
import sqlite3

from .errors import ValidationError
from .models import ValidationResult
from .schema_manager import SchemaManager

FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "pragma",
    "attach",
    "detach",
    "vacuum",
    "begin",
    "commit",
    "rollback",
}


class SQLValidator:
    """Validates SQL before execution."""

    def __init__(self, connection: sqlite3.Connection, schema_manager: SchemaManager):
        self.connection = connection
        self.schema_manager = schema_manager

    def validate(self, sql: str) -> ValidationResult:
        if not sql or not sql.strip():
            return ValidationResult(False, ("SQL query cannot be empty.",), "")

        without_comments = self._strip_comments(sql)
        statements = self._split_sql_statements(without_comments)
        if not statements:
            return ValidationResult(False, ("SQL query cannot be empty.",), "")
        if len(statements) > 1:
            return ValidationResult(
                False,
                ("Only a single SELECT statement is allowed.",),
                statements[0],
            )

        statement = statements[0].strip()
        first_token = self._first_token(statement)
        if first_token not in {"select", "with"}:
            return ValidationResult(
                False,
                ("Only SELECT queries are allowed.",),
                statement,
            )

        if self._contains_forbidden_keyword(statement):
            return ValidationResult(
                False,
                ("Disallowed SQL keyword detected.",),
                statement,
            )

        table_errors = self._validate_tables(statement)
        if table_errors:
            return ValidationResult(False, tuple(table_errors), statement)

        compile_errors = self._validate_compilation(statement)
        if compile_errors:
            return ValidationResult(False, tuple(compile_errors), statement)

        return ValidationResult(True, (), statement)

    def assert_valid(self, sql: str) -> str:
        result = self.validate(sql)
        if not result.is_valid:
            raise ValidationError(result.errors)
        return result.normalized_sql

    def _validate_tables(self, statement: str) -> list[str]:
        referenced_tables = self._extract_table_references(statement)
        if not referenced_tables:
            return []

        known_tables = set(self.schema_manager.list_tables())
        errors: list[str] = []
        for table_name in referenced_tables:
            if table_name not in known_tables:
                errors.append(f"Unknown table referenced: {table_name}")
        return errors

    def _validate_compilation(self, statement: str) -> list[str]:
        try:
            self.connection.execute(f"EXPLAIN QUERY PLAN {statement}")
            return []
        except sqlite3.Error as exc:
            message = str(exc)
            lowered = message.lower()
            if "no such table" in lowered:
                return [f"Unknown table referenced: {message.split(':', 1)[-1].strip()}"]
            if "no such column" in lowered:
                return [f"Unknown column referenced: {message.split(':', 1)[-1].strip()}"]
            return [f"Invalid SQL: {message}"]

    @staticmethod
    def _strip_comments(sql: str) -> str:
        no_line_comments = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
        no_block_comments = re.sub(r"/\*.*?\*/", "", no_line_comments, flags=re.DOTALL)
        return no_block_comments

    @staticmethod
    def _split_sql_statements(sql: str) -> list[str]:
        statements: list[str] = []
        current: list[str] = []
        quote_char: str | None = None
        for char in sql:
            if quote_char is None and char in {"'", '"'}:
                quote_char = char
                current.append(char)
                continue
            if quote_char is not None and char == quote_char:
                quote_char = None
                current.append(char)
                continue
            if quote_char is None and char == ";":
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                continue
            current.append(char)
        trailing = "".join(current).strip()
        if trailing:
            statements.append(trailing)
        return statements

    @staticmethod
    def _first_token(statement: str) -> str:
        match = re.match(r"^\s*([a-zA-Z_]+)", statement)
        return (match.group(1).lower() if match else "").strip()

    @staticmethod
    def _contains_forbidden_keyword(statement: str) -> bool:
        # Remove quoted text to avoid false positives from string literals.
        unquoted = re.sub(r"'[^']*'|\"[^\"]*\"", " ", statement)
        pattern = r"\b(" + "|".join(sorted(FORBIDDEN_KEYWORDS)) + r")\b"
        return re.search(pattern, unquoted, flags=re.IGNORECASE) is not None

    @staticmethod
    def _extract_table_references(statement: str) -> set[str]:
        pattern = re.compile(
            r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\")",
            flags=re.IGNORECASE,
        )
        tables: set[str] = set()
        for match in pattern.findall(statement):
            cleaned = match.strip().strip('"').split(".", 1)[-1]
            tables.add(cleaned)
        return tables
