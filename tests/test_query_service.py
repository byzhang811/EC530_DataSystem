from pathlib import Path

import pandas as pd
import pytest

from datasheet_ai.errors import ValidationError
from datasheet_ai.models import LLMOutput
from datasheet_ai.query_service import QueryService


class FakeLLMAdapter:
    def __init__(self, sql: str):
        self.sql = sql

    def generate_sql(self, user_request, schema):
        return LLMOutput(sql=self.sql, explanation=f"Generated for: {user_request}")


def test_query_service_ingest_and_run_sql(tmp_path: Path):
    db_path = tmp_path / "system.db"
    csv_path = tmp_path / "users.csv"
    pd.DataFrame({"name": ["Alice", "Bob"], "age": [31, 25]}).to_csv(csv_path, index=False)

    service = QueryService(str(db_path), llm_adapter=FakeLLMAdapter('SELECT * FROM "users"'))
    try:
        load_result = service.ingest_csv(str(csv_path), "users", conflict_strategy="rename")
        assert load_result.rows_inserted == 2

        query_result = service.run_sql('SELECT name FROM "users" ORDER BY age DESC')
        assert query_result.columns == ("name",)
        assert query_result.rows == (("Alice",), ("Bob",))
    finally:
        service.close()


def test_query_service_nl_path_uses_validator(tmp_path: Path):
    db_path = tmp_path / "system.db"
    csv_path = tmp_path / "users.csv"
    pd.DataFrame({"name": ["Alice"], "age": [31]}).to_csv(csv_path, index=False)

    service = QueryService(str(db_path), llm_adapter=FakeLLMAdapter('SELECT age FROM "users"'))
    try:
        service.ingest_csv(str(csv_path), "users")
        result = service.ask_natural_language("How old are users?")
        assert result.source == "llm"
        assert result.rows == ((31,),)
    finally:
        service.close()


def test_query_service_rejects_invalid_llm_sql(tmp_path: Path):
    db_path = tmp_path / "system.db"
    csv_path = tmp_path / "users.csv"
    pd.DataFrame({"name": ["Alice"], "age": [31]}).to_csv(csv_path, index=False)

    service = QueryService(
        str(db_path),
        llm_adapter=FakeLLMAdapter('SELECT * FROM "users"; DROP TABLE "users";'),
    )
    try:
        service.ingest_csv(str(csv_path), "users")
        with pytest.raises(ValidationError):
            service.ask_natural_language("Show all users")
    finally:
        service.close()
