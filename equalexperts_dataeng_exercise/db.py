import duckdb

DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"


def create_database_and_schema():
    try:
        # Connect to the database
        conn = duckdb.connect("warehouse.db")
        print("Connected to the database")

        # Create schema 'blog_analysis' if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA_NAME}")
        print("Created schema successfully")
    except Exception as e:
        print(f"Error : {e}")
    finally:
        conn.close()


# def remove_database():
#     try:
#         if os.path.exists(DB_FULL_NAME):
#             os.remove(DB_FULL_NAME)
#             print(f"Database '{DB_FULL_NAME}' deleted successfully.")
#         else:
#             print(f"Database '{DB_FULL_NAME}' does not exist.")
#     except Exception as e:
#         print(f"Error : {e}")


def main_db():
    # remove_database()
    create_database_and_schema()
