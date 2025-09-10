import duckdb

DB_FULL_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "blog_analysis.votes"
VIEW_NAME = "blog_analysis.outlier_weeks"

# View definition for outlier week using multiple CTEs
sql_outlier_query = f"""DROP VIEW IF EXISTS {VIEW_NAME};
    CREATE VIEW {VIEW_NAME} AS

    WITH weekly_voter AS (
        SELECT EXTRACT(YEAR FROM CreationDate) AS year,
        EXTRACT(WEEK FROM CreationDate) AS week_number,
        COUNT(*) AS weekly_voter_count
    FROM {DB_TABLE_NAME}
    GROUP BY year, week_number
    ),

    avg_voter AS (
        SELECT AVG(weekly_voter_count) AS avg_voters_per_week
        FROM weekly_voter
    ),

    outlier_week AS (
        SELECT w.year,
        w.week_number,
        w.weekly_voter_count,
        ROUND(ABS(1 - (w.weekly_voter_count / a.avg_voters_per_week)),1) AS weekly_vote_ratio
    FROM weekly_voter w
    CROSS JOIN avg_voter a)

    SELECT year as Year,
        week_number as WeekNumber,
        weekly_voter_count as VoteCount
    FROM outlier_week
    WHERE weekly_vote_ratio >0.2
    ORDER BY year ASC, week_number ASC
    """


def get_outliers_week():
    # Extract Outlier weeks
    try:
        conn = duckdb.connect(DB_FULL_NAME)
        conn.execute(sql_outlier_query).fetchall()
        view_query = f"SELECT * from {VIEW_NAME}"
        outlier_table = conn.execute(view_query).fetchall()
        column_names = [desc[0] for desc in conn.description]
        print(f"Summary, Number of rows :{len(outlier_table)}")
        print(column_names)

        for r in outlier_table:
            print(r)
    except Exception as e:
        print(f"Error : {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print("Outliers week calculating")
    get_outliers_week()
    print("Final output")
