from datasheet_ai.schema_manager import SchemaManager
from datasheet_ai.sql_validator import SQLValidator


def _setup_users_table(sqlite_conn):
    sqlite_conn.execute(
        'CREATE TABLE "users" (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)'
    )
    sqlite_conn.execute('INSERT INTO "users" (name, age) VALUES ("Alice", 30)')
    sqlite_conn.commit()


def test_validator_accepts_valid_select(sqlite_conn):
    _setup_users_table(sqlite_conn)
    validator = SQLValidator(sqlite_conn, SchemaManager(sqlite_conn))
    result = validator.validate('SELECT name FROM "users" WHERE age > 20')
    assert result.is_valid is True
    assert result.errors == ()


def test_validator_rejects_non_select(sqlite_conn):
    _setup_users_table(sqlite_conn)
    validator = SQLValidator(sqlite_conn, SchemaManager(sqlite_conn))
    result = validator.validate('DELETE FROM "users" WHERE age > 20')
    assert result.is_valid is False
    assert "Only SELECT queries are allowed." in result.errors


def test_validator_rejects_unknown_table(sqlite_conn):
    _setup_users_table(sqlite_conn)
    validator = SQLValidator(sqlite_conn, SchemaManager(sqlite_conn))
    result = validator.validate('SELECT * FROM "orders"')
    assert result.is_valid is False
    assert any(error.startswith("Unknown table referenced:") for error in result.errors)


def test_validator_rejects_unknown_column(sqlite_conn):
    _setup_users_table(sqlite_conn)
    validator = SQLValidator(sqlite_conn, SchemaManager(sqlite_conn))
    result = validator.validate('SELECT salary FROM "users"')
    assert result.is_valid is False
    assert any(error.startswith("Unknown column referenced:") for error in result.errors)


def test_validator_rejects_multiple_statements(sqlite_conn):
    _setup_users_table(sqlite_conn)
    validator = SQLValidator(sqlite_conn, SchemaManager(sqlite_conn))
    result = validator.validate('SELECT * FROM "users"; DROP TABLE "users";')
    assert result.is_valid is False
    assert "Only a single SELECT statement is allowed." in result.errors
