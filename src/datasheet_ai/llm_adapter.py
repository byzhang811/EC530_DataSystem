"""LLM adapter layer that translates natural language requests into SQL."""

from __future__ import annotations

import os
import re

from .models import LLMOutput, TableSchema
from .schema_manager import SchemaManager


class RuleBasedLLMAdapter:
    """
    Fallback adapter used for offline development and tests.
    It mimics an LLM interface while staying deterministic.
    """

    def generate_sql(self, user_request: str, schema: dict[str, TableSchema]) -> LLMOutput:
        if not schema:
            return LLMOutput(sql="SELECT 1 AS no_data_loaded", explanation="No tables loaded.")

        table_name = sorted(schema.keys())[0]
        normalized_request = user_request.lower()
        if "count" in normalized_request or "多少" in normalized_request:
            sql = f'SELECT COUNT(*) AS row_count FROM "{table_name}"'
            explanation = f"Count rows in {table_name}."
        elif "top" in normalized_request or "前" in normalized_request:
            sql = f'SELECT * FROM "{table_name}" LIMIT 5'
            explanation = f"Return top 5 rows from {table_name}."
        else:
            sql = f'SELECT * FROM "{table_name}" LIMIT 10'
            explanation = f"Return sample rows from {table_name}."
        return LLMOutput(sql=sql, explanation=explanation)


class OpenAIAdapter:
    """OpenAI-backed implementation for NL -> SQL translation."""

    def __init__(self, model: str = "gpt-4.1-mini", api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAIAdapter.")

    def generate_sql(self, user_request: str, schema: dict[str, TableSchema]) -> LLMOutput:
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - dependency edge
            raise RuntimeError(
                "openai package is not installed. Install with: pip install '.[llm]'"
            ) from exc

        client = OpenAI(api_key=self.api_key)
        prompt = self.build_prompt(user_request, schema)
        response = client.responses.create(
            model=self.model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "Translate user requests into one SQLite SELECT statement only. "
                        "Never generate data modification SQL."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        raw_output = (response.output_text or "").strip()
        sql = self._extract_sql(raw_output)
        return LLMOutput(sql=sql, explanation=raw_output)

    @staticmethod
    def build_prompt(user_request: str, schema: dict[str, TableSchema]) -> str:
        schema_text = SchemaManager.format_schema_for_prompt(schema)
        return (
            "You are an AI assistant that converts user requests into SQLite SQL.\n\n"
            "Database schema:\n"
            f"{schema_text}\n\n"
            "User request:\n"
            f"{user_request}\n\n"
            "Your task is to:\n"
            "1. Generate one SQL query that answers the user request.\n"
            "2. Ensure the SQL is valid SQLite syntax.\n"
            "3. Return only read-only SQL (SELECT).\n\n"
            "Output format:\n"
            "- SQL Query\n"
            "- Explanation"
        )

    @staticmethod
    def _extract_sql(raw_output: str) -> str:
        fenced = re.search(r"```(?:sql)?\s*(.*?)```", raw_output, flags=re.IGNORECASE | re.DOTALL)
        candidate = fenced.group(1).strip() if fenced else raw_output
        lines = [line.strip() for line in candidate.splitlines() if line.strip()]
        if not lines:
            raise ValueError("LLM response did not contain SQL.")

        if len(lines) == 1:
            return lines[0].rstrip(";")

        sql_lines: list[str] = []
        for line in lines:
            lowered = line.lower()
            if lowered.startswith(("select ", "with ", "from ", "where ", "group ", "order ", "limit ", "join ", "left ", "right ", "inner ", "outer ", "on ", "having ", "union ", "and ", "or ")):
                sql_lines.append(line)
            elif sql_lines:
                break
        if not sql_lines:
            return lines[0].rstrip(";")
        return " ".join(sql_lines).rstrip(";")
