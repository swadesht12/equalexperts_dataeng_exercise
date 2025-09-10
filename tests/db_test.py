import duckdb
import os
import subprocess
import pytest

DB_NAME = "warehouse"
# DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "votes"

# absolute path to the db file inside project folder
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FULL_NAME = os.path.join(PROJECT_DIR, "warehouse.db")


@pytest.fixture(autouse=True)
def clean_db():
    """Ensure DB file is removed before and after each test."""
    if os.path.exists(DB_FULL_NAME):
        os.remove(DB_FULL_NAME)
    yield
    if os.path.exists(DB_FULL_NAME):
        os.remove(DB_FULL_NAME)


def test_duckdb_connection():
    """Basic DuckDB connectivity test."""
    cursor = duckdb.connect(DB_FULL_NAME)
    assert cursor.execute("SELECT 1").fetchall() == [(1,)]
    cursor.close()


# def run_db():
#     """Run the db.py module to set up schema + tables."""
#     result = subprocess.run(
#         args=[
#             "python",
#             "-m",
#             "equalexperts_dataeng_exercise.db",
#         ],
#         capture_output=True,
#         text=True,
#     )
#     if result.returncode != 0:
#         raise RuntimeError(
#             f"DB script failed!\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
#         )

def run_db():
    """Run the db.py module to set up schema + tables."""
    project_root = os.path.dirname(os.path.abspath(__file__))  # C:\Users\swade\equalexperts_dataeng_exercise\tests
    parent_dir = os.path.dirname(project_root)  # C:\Users\swade\equalexperts_dataeng_exercise
    result = subprocess.run(
        args=["python", "-m", "equalexperts_dataeng_exercise.db"],
        cwd=parent_dir,   # 👈 run from project root
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"DB script failed!\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

def test_check_schema_exists():
    """Verify that the expected schema is created."""
    run_db()
    sql = f"""
        SELECT catalog_name, schema_name
        FROM information_schema.schemata
        WHERE schema_name = '{DB_SCHEMA_NAME}' AND catalog_name = '{DB_NAME}';
    """
    con = duckdb.connect(DB_FULL_NAME, read_only=True)
    result = con.sql(sql).fetchall()
    con.close()

    assert len(result) == 1, f"Expected schema '{DB_SCHEMA_NAME}' in catalog '{DB_NAME}'"
    assert result[0][0] == DB_NAME
    assert result[0][1] == DB_SCHEMA_NAME


def test_check_table_exists():
    """Verify that the expected table exists inside the schema."""
    run_db()
    sql = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = '{DB_SCHEMA_NAME}' AND table_name = '{DB_TABLE_NAME}';
    """
    con = duckdb.connect(DB_FULL_NAME, read_only=True)
    result = con.sql(sql).fetchall()
    con.close()

    assert len(result) == 1, f"Expected table '{DB_TABLE_NAME}' in schema '{DB_SCHEMA_NAME}'"
    assert result[0][0] == DB_SCHEMA_NAME
    assert result[0][1] == DB_TABLE_NAME
