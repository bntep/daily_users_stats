"""
This sript create a ETL instance where steps involved in producing the end-user data is defined.
Typical usage example:

    etl_pipeline = EtlPipeline(source_db_alias='mydatabase', target_db_alias='mydatabase')
    etl_pipeline.run()

----
Logging:
- Log entries are written to a file named 'logfile.log'.
- Every step in the ETL process is logged.

NOTE:

"""
import os
import sys
from pathlib import Path
sys.path.append(str(Path(os.getcwd())))

import re
import logging
import pandas as pd
import uuid

sys.path.append(str(Path(__file__).parent.parent))
from utils.dbclient.DatabaseClient import DbConnector


def sanitize_filename(test_str: str) -> str:
    """
    Replace all invalid characters from filename with underscore and ensure it is a valid file path.
    """
    if not isinstance(test_str, str):
        raise TypeError(f"Expected Path, got {type(test_str)}")

    # Replace all invalid characters with underscore
    cleaned_filename = re.sub(r"[^\w\-. ]", "_", test_str)

    # If filename is empty after sanitization, provide a default one
    if not cleaned_filename.strip("_ "):
        cleaned_filename = "empty_name_" + str(uuid.uuid4())

    # Convert to a valid file path
    cleaned_filename = os.path.normpath(cleaned_filename)

    return cleaned_filename


def extract_data_from_source(
    db_connector: DbConnector, source_table_name: str, filename: str, data_path: Path = Path.cwd() / "data"
) -> pd.DataFrame:
    """
    Extract data from source database.
    :param db_connector: DbConnector instance
    :param source_table_name: str
    :return: pd.DataFrame
    """
    query = f"SELECT * FROM {source_table_name}"
    logging.info("Extracting data from source database...")

    df = db_connector.execute_query(query)
    saved_at = store_data_to_csv(
        df=df, store_path= data_path / sanitize_filename(filename)
    )
    print(f"Data extracted from source database and saved at {saved_at}")
    logging.info("Data extracted from source database and saved at %s", saved_at)

    return df.copy()


def store_data_to_csv(df: pd.DataFrame, store_path: Path) -> str:
    """
    Save a DataFrame to csv format at the given path.
    """
    if not isinstance(store_path, Path):
        raise TypeError(f"Expected Path, got {type(store_path)}")

    # check if the file path has .csv extension or not
    if not str(store_path).endswith(".csv"):
        store_path += ".csv"

    if not os.path.exists(store_path.parent):
        store_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Save DataFrame to csv
        df.to_csv(f"{store_path}", index=False, encoding="utf-8")
        print(f"File saved successfully at {store_path}")
    except Exception as e:
        print(f"An error occurred while saving the file: {str(e)}")

    return store_path

def load_data_from_source(db_name: str, source_table_name: str, filename: str, load_from_system_if_exists: bool = False, data_path: Path = Path.cwd() / "data"):
    if os.path.exists(data_path / filename) and load_from_system_if_exists:
        return pd.read_csv(data_path / filename)
    else:
        return extract_data_from_source(
            db_connector=DbConnector(db_name),
            source_table_name=source_table_name,
            filename=filename,
            data_path=data_path
        )
    

    

if __name__ == "__main__":
    df_test = load_data_from_source(db_name="durango",
                                    source_table_name="esg_ref_clarity_metrics_def",
                                    filename="esg_ref_clarity_metrics_def.csv",
                                    load_from_system_if_exists=True,
                                    data_path = Path.cwd() / "tititoto"
    )
    
    print(df_test.head())
