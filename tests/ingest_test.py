import logging
import os
import subprocess
import time
import duckdb
import pytest
import json
import equalexperts_dataeng_exercise.ingest as ingest

logger = logging.getLogger()

DB_NAME = "warehouse"
DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "votes"
DB_TABLE_FULL_NAME = "blog_analysis.votes"
FILE_NAME = "uncommitted/votes.jsonl"


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists(DB_FULL_NAME):
        os.remove(DB_FULL_NAME)


def run_ingestion() -> float:
    # Returns time in seconds that the ingestion process took to run
    
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            f"{FILE_NAME}",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def test_check_table_exists_and_names():
    run_ingestion()
    sql = f"""
        SELECT table_name, table_schema, table_catalog
        FROM information_schema.tables
        WHERE table_type LIKE '%TABLE' AND table_name='{DB_TABLE_NAME}' AND table_schema='{DB_SCHEMA_NAME}' AND table_catalog='{DB_NAME}';
    """
    con = duckdb.connect(DB_FULL_NAME, read_only=True)
    result = con.sql(sql).fetchall()
    res_value = result[0]
    assert len(result) == 1, "Expected table 'votes' to exist"
    assert res_value[0] == "votes", "Expected table name to be 'votes'"
    assert res_value[1] == "blog_analysis", "Expected schema name to be 'blog_analysis'"
    assert res_value[2] == "warehouse", "Expected catalog name to be 'warehouse'"
    con.close()


def count_rows_in_data_file():
    with open(FILE_NAME, "r", encoding="utf-8") as data:
        return sum(1 for _ in data)

def is_valid_json():
    invalid_lines = []

    try:
        with open(FILE_NAME, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    invalid_lines.append(i)
    except FileNotFoundError:
        print(f"File not found: {FILE_NAME}")
    except Exception as e:
        print(f"Unexpected error while validating JSON: {e}")


def test_check_correct_number_of_rows_after_ingesting_once():
    sql = f"SELECT COUNT(*) FROM {DB_TABLE_FULL_NAME}"
    time_taken_seconds = run_ingestion()
    assert time_taken_seconds < 2, "Ingestion solution is too slow!"
    con = duckdb.connect(DB_FULL_NAME, read_only=True)
    result = con.execute(sql).fetchall()
    count_in_db = result[0][0]
    assert (
        count_in_db == count_rows_in_data_file()
    ), "Expect same count in db as in input file"
    con.close()


def test_check_correct_number_of_rows_after_ingesting_twice():
    sql = f"SELECT COUNT(*) FROM {DB_TABLE_FULL_NAME}"
    for _ in range(2):
        run_ingestion()
    con = duckdb.connect(DB_FULL_NAME, read_only=True)
    result = con.execute(sql).fetchall()
    count_in_db = result[0][0]
    assert (
        count_in_db == count_rows_in_data_file()
    ), "Expect same count in db as in input file if processed twice"
    con.close()


def test_check_error_insert_data_into_database():
    # Test exception handling by using a dummy file or file name which is not in existence to check exception handling test
    
    with pytest.raises(Exception) as e:
        ingest.insert_data_into_database('error_file_name.jsonl')
    assert e.typename == 'CatalogException', "Expecting a CatalogException : Table with name 'votes' does not exist!"


def test_check_error_display_data():
    # Exception handling test in display_data function
    
    with pytest.raises(Exception) as e:
        ingest.display_data(20)
    assert e.typename == 'CatalogException', "Expecting a CatalogException : Table with name 'votes' does not exist!"

def test_check_table_schema_and_constraints():
    run_ingestion()
    con = duckdb.connect(DB_FULL_NAME, read_only=True)

    # Check column count and names
    column_info_sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{DB_SCHEMA_NAME}' AND table_name = '{DB_TABLE_NAME}';
    """
    columns = con.execute(column_info_sql).fetchall()
    assert len(columns) == 4, f"Expected 4 columns in table '{DB_TABLE_NAME}', found {len(columns)}"

    column_names = [col[0] for col in columns]
    assert "Id" in column_names, "Expected column 'Id' to exist"
    assert "CreationDate" in column_names, "Expected column 'CreationDate' to exist"

    # Check CreationDate is TIMESTAMP
    creation_date_type = next((col[1] for col in columns if col[0] == "CreationDate"), None)
    assert creation_date_type.upper() == "TIMESTAMP", f"Expected 'CreationDate' to be TIMESTAMP, found {creation_date_type}"

    con.close()
