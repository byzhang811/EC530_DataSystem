import pandas as pd

from datasheet_ai.schema_manager import SchemaManager, normalize_identifier


def test_normalize_identifier_handles_spaces_and_case():
    assert normalize_identifier(" User Name ") == "user_name"


def test_infer_schema_and_create_table(sqlite_conn):
    manager = SchemaManager(sqlite_conn)
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [30, 20],
            "Revenue": [10.5, 9.75],
        }
    )

    inferred = manager.infer_schema_from_dataframe("users", df)
    assert [c.name for c in inferred.columns] == ["name", "age", "revenue"]
    assert [c.data_type for c in inferred.columns] == ["TEXT", "INTEGER", "REAL"]

    manager.create_table("users", inferred)
    schema = manager.get_table_schema("users")
    assert schema is not None
    assert [col.name for col in schema.columns] == ["id", "name", "age", "revenue"]
    assert schema.columns[0].data_type == "INTEGER"


def test_schema_compatibility_ignores_id(sqlite_conn):
    manager = SchemaManager(sqlite_conn)
    existing_df = pd.DataFrame({"Name": ["A"], "Age": [1]})
    inferred_df = pd.DataFrame({"name": ["B"], "age": [2]})
    changed_df = pd.DataFrame({"name": ["B"], "salary": [2.0]})

    existing = manager.infer_schema_from_dataframe("users", existing_df)
    manager.create_table("users", existing)
    persisted = manager.get_table_schema("users")
    compatible = manager.infer_schema_from_dataframe("users", inferred_df)
    incompatible = manager.infer_schema_from_dataframe("users", changed_df)

    assert persisted is not None
    assert manager.schemas_compatible(persisted, compatible)
    assert not manager.schemas_compatible(persisted, incompatible)
