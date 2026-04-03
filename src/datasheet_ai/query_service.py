"""Service layer coordinating ingestion, validation, LLM translation, and query execution."""

from __future__ import annotations

from pathlib import Path
import sqlite3

from .csv_loader import CSVLoader
from .llm_adapter import RuleBasedLLMAdapter
from .models import QueryResult, TableSchema
from .schema_manager import SchemaManager
from .sql_validator import SQLValidator


class QueryService:
    """Single gateway used by the CLI and other clients."""

    def __init__(self, db_path: str, llm_adapter=None, error_log_path: str = "error_log.txt"):
        self.db_path = db_path
        self._ensure_db_directory(db_path)
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row

        self.schema_manager = SchemaManager(self.connection)
        self.csv_loader = CSVLoader(
            connection=self.connection,
            schema_manager=self.schema_manager,
            error_log_path=error_log_path,
        )
        self.validator = SQLValidator(self.connection, self.schema_manager)
        self.llm_adapter = llm_adapter or RuleBasedLLMAdapter()

    def close(self) -> None:
        self.connection.close()

    def list_tables(self) -> list[str]:
        return self.schema_manager.list_tables()

    def get_database_schema(self) -> dict[str, TableSchema]:
        return self.schema_manager.get_database_schema()

    def ingest_csv(
        self,
        csv_path: str,
        table_name: str,
        conflict_strategy: str = "rename",
    ):
        return self.csv_loader.load_csv(
            csv_path=csv_path,
            table_name=table_name,
            conflict_strategy=conflict_strategy,
        )

    def detect_schema_conflict(self, csv_path: str, table_name: str) -> bool:
        return self.csv_loader.detect_schema_conflict(csv_path, table_name)

    def run_sql(self, sql: str, source: str = "user_sql") -> QueryResult:
        normalized_sql = self.validator.assert_valid(sql)
        cursor = self.connection.execute(normalized_sql)
        rows = cursor.fetchall()
        columns = tuple(description[0] for description in (cursor.description or ()))
        row_values = tuple(tuple(row) for row in rows)
        return QueryResult(columns=columns, rows=row_values, sql=normalized_sql, source=source)

    def ask_natural_language(self, user_request: str) -> QueryResult:
        schema = self.get_database_schema()
        llm_output = self.llm_adapter.generate_sql(user_request, schema)
        result = self.run_sql(llm_output.sql, source="llm")
        return QueryResult(
            columns=result.columns,
            rows=result.rows,
            sql=result.sql,
            source="llm",
            llm_explanation=llm_output.explanation,
        )

    @staticmethod
    def _ensure_db_directory(db_path: str) -> None:
        db_parent = Path(db_path).expanduser().resolve().parent
        db_parent.mkdir(parents=True, exist_ok=True)
