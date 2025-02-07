"""
This sript facilitate the connection to a PostgreSQL database.
Typical usage example:

    connector = DbConnector('mydatabase')
    results = connector.execute_query("SELECT * FROM my_table")
    print(results)

---
DbConnector('durango') 
df = execute_query(self, query: str) -> pd.DataFrame

----
Logging:
- Log entries are written to a file named 'logfile.log'.

NOTE:
Be careful with the database alias and SQL queries.
The alias must match the set environment variables.
"""

import os
import sys
from pathlib import Path
import logging
import psycopg2
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from utils.LogWriter import log_location
import pandas as pd
sys.path.append(str(Path(os.getcwd())))
from module.env_db_conn import *


def log_config(log_path: Path, name = __name__ ) -> object:
    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s -%(levelname)s - %(message)s', 
        datefmt='%Y%m%d %H:%M:%S',
        filemode='a',
        )

    logging.getLogger('sqlalchemy.engine')
    db_logger = logging.getLogger(name)

    return db_logger

db_logger = log_config(log_location())

class DbConnector:
    def __init__(self, db_alias: str, echo: bool = False):
        db_alias = db_alias.upper()
        self.drivername = globals().get(f'{db_alias}_DRIVERNAME')
        if not self.drivername:
            raise Exception(f"Variable '{db_alias}_DRIVERNAME' not set in env.py")
    
        self.dbalias = db_alias
        self.dbname = globals().get(f'{db_alias}_DATABASE')
        self.user = globals().get(f'{db_alias}_USERNAME')
        self.password = globals().get(f'{db_alias}_PASSWORD')
        self.host = globals().get(f'{db_alias}_HOST')


        if not all([self.dbname, self.user, self.host]):
            raise Exception('One or more database connection variables not set in env.py')

        self.url = URL.create(drivername=self.drivername,
                              username=self.user,
                              password=self.password,
                              host=self.host,
                              database=self.dbname)
        try:
            db_logger.info('Connecting to the PostgreSQL database... %s', self.dbname)
            self.engine = create_engine(self.url, echo=echo)
            self.Session = sessionmaker(bind=self.engine)

        except Exception as e:
            print("Error while connecting to PostgreSQL", e)
            db_logger.error("Error while connecting to PostgreSQL %s", sys.exc_info()[1])
            sys.exit()
    
    def __repr__(self):
        return f"DbConnector(**{self.__dict__})"

    def execute(self, query: str) -> None:
        with self.engine.connect() as connection:
            if not query:
                raise ValueError("Query cannot be empty or None")
            else:
                # Check if the query is successful
                try:
                    connection.execute(sqlalchemy.text(query))
                    db_logger.info("Query executed successfully.")
                except psycopg2.errors.UndefinedTable as e:
                    print("The table does not exist: ", e)
                except sqlalchemy.exc.ProgrammingError as e:
                    print("There is a syntax error in your SQL command: ", e)

    def execute_query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as connection:
            if not query:
                raise ValueError("Query cannot be empty or None")
            else:
                # Check if the query is successful
                try:
                    df = pd.read_sql_query(sqlalchemy.text(query), connection)
                    if df is not None:
                        db_logger.info("Query executed successfully.")
                        return df
                    else:
                        db_logger.error("Query failed.")
                        raise ValueError(f"Query failed: {query}")
                except psycopg2.errors.UndefinedTable as e:
                    print("The table does not exist: ", e)
                    return pd.DataFrame()
                except sqlalchemy.exc.ProgrammingError as e:
                    print("There is a syntax error in your SQL command: ", e)
                    return pd.DataFrame()
                except Exception as e:
                    print("An error occurred while executing the query: ", e)
                    db_logger.error("An error occurred while executing the query: %s", sys.exc_info()[1])
                    return pd.DataFrame()

    def execute_query_with_params(self, query: str, params: dict) -> pd.DataFrame:
        with self.engine.connect() as connection:
            if not query:
                raise ValueError("Query cannot be empty or None")
            else:
                # Check if the query is successful
                try:
                    df = pd.read_sql_query(sqlalchemy.text(query), connection, params=params)
                    if df is not None:
                        db_logger.info("Query executed successfully.")
                        return df
                    else:
                        db_logger.error("Query failed.")
                        raise ValueError(f"Query failed: {query}")
                except psycopg2.errors.UndefinedTable as e:
                    print("The table does not exist: ", e)
                    return pd.DataFrame()
                except sqlalchemy.exc.ProgrammingError as e:
                    print("There is a syntax error in your SQL command: ", e)
                    return pd.DataFrame()
                except Exception as e:
                    print("An error occurred while executing the query: ", e)
                    db_logger.error("An error occurred while executing the query: %s", sys.exc_info()[1])
                    return pd.DataFrame()
    
    
    def table_exists(self, table_name: str) -> bool:
        if not table_name:
            raise ValueError("Table name cannot be empty or None")  
        with self.engine.connect() as connection:          
            return connection.dialect.has_table(connection, table_name)
           
    
    def close(self):
        self.engine.dispose()
        db_logger.info("Database connection closed.")
        print("Database connection closed.")

if __name__ == '__main__':
    results = DbConnector('durango').execute_query("SELECT * FROM information_schema.tables WHERE table_name LIKE 'esg%';")  # This might return multiple columns
    print(results)