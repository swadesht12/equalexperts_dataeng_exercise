import sys
import duckdb
import equalexperts_dataeng_exercise.db as db

DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "blog_analysis.votes"


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


def insert_data_into_database(file_name):
    """ To insert data into table """
    try:
        print(f"Inserting data from file: {file_name}")
        insert = f"""INSERT INTO {DB_TABLE_NAME}
            SELECT Distinct(ID), PostId, VoteTypeId, CreationDate FROM '{file_name}'"""
        # print("Insert query:", insert)
        conn = duckdb.connect(DB_FULL_NAME)
        res = conn.execute(insert).fetchall()
        print(
            f"Data inserted successfully:'{DB_TABLE_NAME}' and number of rows:{res[0][0]}")
    except Exception as e:
        print(f"Error : {e}")
        raise
    finally:
        conn.close()


def display_data(rows=20):

    try:
        conn = duckdb.connect(DB_FULL_NAME)
        sql_query = f"SELECT * FROM {DB_TABLE_NAME} ORDER BY ID DESC LIMIT 20"
        data_rows = conn.execute(sql_query).fetchall()

        # Print the fetched rows
        print(f"Displaying rows from the table : {DB_TABLE_NAME}")
        for row in data_rows:
            print(row)
    except Exception as e:
        print(f"Error : {e}")
        raise
    finally:
        conn.close()


def main_ingestion():
    try:
        db.main_db()
        create_table()
        file_name = sys.argv[1]
        # file_name = "uncommitted/votes.jsonl"
        print(f"File name :{file_name}")
        insert_data_into_database(file_name)
        display_data(20)
    except Exception as e:
        print(f"Error : {e}")


if __name__ == "__main__":
    print("Start data Ingestion")
    main_ingestion()
    print("End Ingestion")
