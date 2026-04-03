# AI Usage Record

This document records where AI assistance was used during development.

## Where AI Was Used

- Architecture review for modular boundaries.
- Discussion of SQL validation strategy tradeoffs.
- Prompt structure ideas for NL-to-SQL adapter.
- Code review suggestions for test coverage and error handling.

## Guardrails Followed

- Unit test behavior was defined first for critical validator requirements.
- No generated code was accepted without manual review and edits.
- LLM output is treated as untrusted input in runtime system design.

## Required Case: LLM-Generated Code Was Wrong

During validator iteration, an AI-assisted draft only checked that the query started with `SELECT`, but did not reject multi-statement payloads such as:

```sql
SELECT * FROM "users"; DROP TABLE "users";
```

This issue was caught by test:

- `tests/test_sql_validator.py::test_validator_rejects_multiple_statements`

After failure, validator logic was refined to split SQL safely and reject any query with more than one statement. The final implementation now blocks this payload before execution.
