"""CSV ingestion into SQLite with explicit schema handling."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd

from .models import LoadResult
from .schema_manager import SchemaManager, normalize_identifier, quote_identifier


class CSVLoader:
    """Loads CSV files into SQLite without using dataframe.to_sql()."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        schema_manager: SchemaManager,
        error_log_path: str = "error_log.txt",
    ):
        self.connection = connection
        self.schema_manager = schema_manager
        self.error_log_path = Path(error_log_path)

    def load_csv(
        self,
        csv_path: str,
        table_name: str,
        conflict_strategy: str = "rename",
    ) -> LoadResult:
        """
        Load a CSV file into a table.

        conflict_strategy:
        - rename: create a new table name if schema conflicts
        - overwrite: drop and recreate the table
        - skip: skip ingestion on schema conflict
        """
        try:
            csv_file = Path(csv_path)
            if not csv_file.exists():
                raise FileNotFoundError(f"CSV file does not exist: {csv_path}")

            normalized_table_name = normalize_identifier(table_name)
            dataframe = pd.read_csv(csv_file)
            dataframe = self._normalize_columns(dataframe)
            inferred_schema = self.schema_manager.infer_schema_from_dataframe(
                normalized_table_name, dataframe
            )

            if not self.schema_manager.table_exists(normalized_table_name):
                self.schema_manager.create_table(normalized_table_name, inferred_schema)
                inserted = self._insert_rows(normalized_table_name, inferred_schema, dataframe)
                return LoadResult(
                    table_name=normalized_table_name,
                    rows_inserted=inserted,
                    created_new_table=True,
                    message=f"Created table '{normalized_table_name}' and inserted {inserted} rows.",
                )

            existing_schema = self.schema_manager.get_table_schema(normalized_table_name)
            if existing_schema and self.schema_manager.schemas_compatible(existing_schema, inferred_schema):
                inserted = self._insert_rows(normalized_table_name, inferred_schema, dataframe)
                return LoadResult(
                    table_name=normalized_table_name,
                    rows_inserted=inserted,
                    created_new_table=False,
                    message=f"Appended {inserted} rows to existing table '{normalized_table_name}'.",
                )

            strategy = conflict_strategy.strip().lower()
            if strategy == "skip":
                return LoadResult(
                    table_name=normalized_table_name,
                    rows_inserted=0,
                    created_new_table=False,
                    message=f"Skipped loading '{csv_path}' because schema does not match table '{normalized_table_name}'.",
                )
            if strategy == "overwrite":
                self.schema_manager.drop_table(normalized_table_name)
                self.schema_manager.create_table(normalized_table_name, inferred_schema)
                inserted = self._insert_rows(normalized_table_name, inferred_schema, dataframe)
                return LoadResult(
                    table_name=normalized_table_name,
                    rows_inserted=inserted,
                    created_new_table=True,
                    message=f"Overwrote table '{normalized_table_name}' and inserted {inserted} rows.",
                )
            if strategy == "rename":
                new_table_name = self.schema_manager.get_non_conflicting_table_name(
                    normalized_table_name
                )
                self.schema_manager.create_table(new_table_name, inferred_schema)
                inserted = self._insert_rows(new_table_name, inferred_schema, dataframe)
                return LoadResult(
                    table_name=new_table_name,
                    rows_inserted=inserted,
                    created_new_table=True,
                    message=(
                        f"Schema conflict detected. Created '{new_table_name}' and inserted "
                        f"{inserted} rows."
                    ),
                )

            raise ValueError(
                "Invalid conflict_strategy. Use one of: rename, overwrite, skip."
            )
        except Exception as exc:  # pragma: no cover - behavior tested via output state
            self._log_error(f"[CSVLoader] {exc}")
            raise

    def detect_schema_conflict(self, csv_path: str, table_name: str) -> bool:
        """Return True when target table exists and CSV schema is incompatible."""
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file does not exist: {csv_path}")

        normalized_table_name = normalize_identifier(table_name)
        if not self.schema_manager.table_exists(normalized_table_name):
            return False

        dataframe = pd.read_csv(csv_file)
        dataframe = self._normalize_columns(dataframe)
        inferred_schema = self.schema_manager.infer_schema_from_dataframe(
            normalized_table_name, dataframe
        )
        existing_schema = self.schema_manager.get_table_schema(normalized_table_name)
        if existing_schema is None:
            return False
        return not self.schema_manager.schemas_compatible(existing_schema, inferred_schema)

    def _insert_rows(
        self,
        table_name: str,
        inferred_schema,
        dataframe: pd.DataFrame,
    ) -> int:
        if dataframe.empty:
            return 0

        column_names = [column.name for column in inferred_schema.columns]
        placeholders = ", ".join(["?"] * len(column_names))
        quoted_columns = ", ".join(quote_identifier(name) for name in column_names)
        insert_sql = (
            f"INSERT INTO {quote_identifier(table_name)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )

        values = [
            tuple(self._coerce_value(value) for value in row)
            for row in dataframe.itertuples(index=False, name=None)
        ]
        self.connection.executemany(insert_sql, values)
        self.connection.commit()
        return len(values)

    def _normalize_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        normalized_names: list[str] = []
        seen: dict[str, int] = {}
        for raw_name in dataframe.columns:
            base = normalize_identifier(str(raw_name))
            count = seen.get(base, 0)
            seen[base] = count + 1
            normalized_names.append(base if count == 0 else f"{base}_{count}")
        renamed = dataframe.copy()
        renamed.columns = normalized_names
        return renamed

    @staticmethod
    def _coerce_value(value):
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value

    def _log_error(self, message: str) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.error_log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp} {message}\n")
