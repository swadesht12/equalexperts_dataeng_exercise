import os
import duckdb

# DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "blog_analysis.votes"

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FULL_NAME = os.path.join(PROJECT_DIR, "..", "warehouse.db")


def create_database_and_schema():
    """Create the DuckDB database and schema if not exists."""
    try:
        conn = duckdb.connect(DB_FULL_NAME)
        print("Connected to the database")
        # Create schema 'blog_analysis' if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA_NAME}")
        print("Created schema successfully")
    except Exception as e:
        print(f"Error : {e}")
    finally:
        conn.close()


def create_table():
    # DB connection and Table creation with Schema
    try:
        conn = duckdb.connect(DB_FULL_NAME)
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME}(
                Id INTEGER PRIMARY KEY,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            );
        ''')
        print(f"Table : '{DB_TABLE_NAME}' created")
    except Exception as e:
        print(f"Error : {e}")
    finally:
        conn.close()


def remove_database():
    """Delete the DuckDB database file if it exists."""
    try:
        if os.path.exists(DB_FULL_NAME):
            os.remove(DB_FULL_NAME)
            print(f"Database '{DB_FULL_NAME}' deleted successfully.")
        else:
            print(f"Database '{DB_FULL_NAME}' does not exist.")
    except Exception as e:
        print(f"Error : {e}")


def main_db():
    """Remove old DB and create fresh schema."""
    remove_database()
    create_database_and_schema()
    create_table()


if __name__ == "__main__":
    main_db()
