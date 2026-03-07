import sqlite3
import csv

def run_analytics():
    tasks={
        "language_distribution.csv":"""SELECT language, COUNT(*) as repo_count
                                    FROM github_repositories
                                    GROUP BY language
                                    ORDER BY repo_count DESC""",
        "repositories_by_year.csv":"""
                                  SELECT repo_year, COUNT(*) as repo_count
                                    FROM github_repositories
                                    GROUP BY repo_year
                                    ORDER BY repo_year
                                    """,
        "top_repositories.csv":"""
                                  SELECT repo_name, owner, stargazers
                                    FROM github_repositories
                                    ORDER BY stargazers DESC
                                    LIMIT 10
                                    """                                                        
    }
    connection=sqlite3.connect('github_data.db')
    print(f"Started analytics part")
    try:
        cursor=connection.cursor();
        for filename,task in tasks.items():
            print(f"processing file: {filename}")
            cursor.execute(task)
            headers=[x[0] for x in cursor.description]
            with open(filename,mode='w',newline="") as f:
                writer=csv.writer(f)
                writer.writerow(headers)
                writer.writerows(cursor.fetchall())

    except sqlite3.Error as e:
       print(f"check the Db connection")
    finally:
        connection.close()
