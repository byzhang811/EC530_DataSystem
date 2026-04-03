from pathlib import Path

import pandas as pd

from datasheet_ai.csv_loader import CSVLoader
from datasheet_ai.schema_manager import SchemaManager


def test_load_csv_creates_table_and_inserts_rows(sqlite_conn, tmp_path: Path):
    csv_path = tmp_path / "users.csv"
    pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]}).to_csv(csv_path, index=False)

    manager = SchemaManager(sqlite_conn)
    loader = CSVLoader(sqlite_conn, manager, error_log_path=str(tmp_path / "error_log.txt"))
    result = loader.load_csv(str(csv_path), "users", conflict_strategy="rename")

    assert result.created_new_table is True
    assert result.table_name == "users"
    assert result.rows_inserted == 2

    rows = sqlite_conn.execute('SELECT name, age FROM "users" ORDER BY id').fetchall()
    assert [tuple(row) for row in rows] == [("Alice", 30), ("Bob", 25)]


def test_load_csv_appends_when_schema_matches(sqlite_conn, tmp_path: Path):
    csv_path_1 = tmp_path / "first.csv"
    csv_path_2 = tmp_path / "second.csv"
    pd.DataFrame({"name": ["Alice"], "age": [30]}).to_csv(csv_path_1, index=False)
    pd.DataFrame({"name": ["Bob"], "age": [25]}).to_csv(csv_path_2, index=False)

    manager = SchemaManager(sqlite_conn)
    loader = CSVLoader(sqlite_conn, manager, error_log_path=str(tmp_path / "error_log.txt"))
    loader.load_csv(str(csv_path_1), "users")
    result = loader.load_csv(str(csv_path_2), "users")

    assert result.created_new_table is False
    assert result.rows_inserted == 1
    total = sqlite_conn.execute('SELECT COUNT(*) FROM "users"').fetchone()[0]
    assert total == 2


def test_load_csv_renames_table_on_schema_conflict(sqlite_conn, tmp_path: Path):
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    pd.DataFrame({"name": ["Alice"], "age": [30]}).to_csv(csv_a, index=False)
    pd.DataFrame({"name": ["Alice"], "salary": [100.0]}).to_csv(csv_b, index=False)

    manager = SchemaManager(sqlite_conn)
    loader = CSVLoader(sqlite_conn, manager, error_log_path=str(tmp_path / "error_log.txt"))
    first = loader.load_csv(str(csv_a), "people", conflict_strategy="rename")
    second = loader.load_csv(str(csv_b), "people", conflict_strategy="rename")

    assert first.table_name == "people"
    assert second.table_name == "people_1"
    tables = set(manager.list_tables())
    assert tables == {"people", "people_1"}


def test_load_csv_skip_on_schema_conflict(sqlite_conn, tmp_path: Path):
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    pd.DataFrame({"name": ["Alice"], "age": [30]}).to_csv(csv_a, index=False)
    pd.DataFrame({"name": ["Alice"], "salary": [100.0]}).to_csv(csv_b, index=False)

    manager = SchemaManager(sqlite_conn)
    loader = CSVLoader(sqlite_conn, manager, error_log_path=str(tmp_path / "error_log.txt"))
    loader.load_csv(str(csv_a), "people", conflict_strategy="rename")
    result = loader.load_csv(str(csv_b), "people", conflict_strategy="skip")

    assert result.rows_inserted == 0
    assert "Skipped loading" in result.message


def test_detect_schema_conflict(sqlite_conn, tmp_path: Path):
    csv_a = tmp_path / "a.csv"
    csv_b = tmp_path / "b.csv"
    pd.DataFrame({"name": ["Alice"], "age": [30]}).to_csv(csv_a, index=False)
    pd.DataFrame({"name": ["Alice"], "salary": [100.0]}).to_csv(csv_b, index=False)

    manager = SchemaManager(sqlite_conn)
    loader = CSVLoader(sqlite_conn, manager, error_log_path=str(tmp_path / "error_log.txt"))
    loader.load_csv(str(csv_a), "people")

    assert loader.detect_schema_conflict(str(csv_a), "people") is False
    assert loader.detect_schema_conflict(str(csv_b), "people") is True
