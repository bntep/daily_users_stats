import sys
import os
from pathlib import Path

sys.path.append(str(Path(os.getcwd())))

from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from utils.dbclient.DatabaseClient import DbConnector

def drop_tables(engine, pattern: str = 'six_ref%'):
    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Fetch and print table names matching the pattern
        stmt = text(f"SELECT tablename FROM pg_tables WHERE tablename ILIKE :pattern")
        tables = [row[0] for row in session.execute(stmt, {'pattern': pattern}).fetchall()]
        
        if not tables:
            print(f"\nNo tables found matching {pattern} pattern.")
            return

        print(f"\nThe following {len(tables)} tables will be dropped:\n" + "\n".join(tables))

        if len(tables) > 15:
            if input("Enter password to confirm: ") != '123':
                print("Wrong password. Aborting script.")
                return

        # Drop tables
        for table in tables:
            session.execute(text(f"DROP TABLE {table}"))
        session.commit()
        print("\nTables dropped successfully.")


if __name__ == '__main__':
    # results = DbConnector('durango').execute_query("SELECT * FROM information_schema.tables WHERE table_name LIKE 'esg%';")  # This might return multiple columns
    # print(results)

    drop_tables(DbConnector('durango').engine, 'src_newost_final%')