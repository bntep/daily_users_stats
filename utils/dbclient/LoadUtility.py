import os
import sys
from pathlib import Path
sys.path.append(str(Path(os.getcwd())))

from utils.LogWriter import log_args
import pandas as pd
from typing import List, Protocol
from sqlalchemy import inspect

from typing import Optional
import re
import pandas as pd
from datetime import datetime

class DataFrameProtocol(Protocol):
    @property
    def columns(self) -> List[str]: ...

class ColumnsProtocol(Protocol):
    def __getitem__(self, index: int) -> str: ...
    def __len__(self) -> int: ...

class TypesProtocol(Protocol):
    def __getitem__(self, index: int) -> type: ...
    def __len__(self) -> int: ...

class SeriesProtocol(Protocol):
    def astype(self, dtype: str) -> 'SeriesProtocol': ...
    @property
    def dtype(self) -> str: ...
    @property
    def str(self) -> 'SeriesProtocol': ...
    
@log_args
def check_columns_structure(df: DataFrameProtocol, columns: ColumnsProtocol) -> bool:
    """
    Check if the dataframe contains the columns in the right order and with the right name
    """
    if not all(column in df.columns for column in columns):
        raise ValueError(f"columns {columns} not in DataFrame columns {df.columns}")
    # Check if the columns are in the right order
    if not all(df.columns[i] == columns[i] for i in range(len(columns))):
        raise ValueError(f"columns {columns} not in the right order, got {df.columns}")
    return True


@log_args
def check_types(df: pd.DataFrame, types: List[type]) -> bool:
    """
    Check if the dataframe contains the columns with the right types
    """
    type_map = {
        int: 'int64',
        float: 'float64',
        str: 'object',
        bool: 'bool',
        pd.Timestamp: 'datetime64[ns]'
    }
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df must be a pandas DataFrame, got {type(df)}")
    if not isinstance(types, list):
        raise TypeError(f"types must be a list, got {type(types)}")
    if not all(isinstance(type_, type) for type_ in types):
        raise TypeError(f"types must be a list of types, got {types}")

    # Map expected types to pandas dtypes
    expected_dtypes = [type_map[type_] for type_ in types]
    
    # Check each column's dtype
    for i in range(len(types)):
        if df.dtypes[i] != expected_dtypes[i]:
            raise ValueError(f"types not in DataFrame types, expected {expected_dtypes}, got {df.dtypes}")
    return True

def pandas_dtype_to_postgres(df, column):
    dtype = df[column].dtype
    if pd.api.types.is_integer_dtype(dtype):
        if df[column].astype(str).str.len().max() > 9:
            return "BIGINT"
        elif df[column].astype(str).str.len().max() > 4:
            return "INTEGER"
        else:
            return "SMALLINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_string_dtype(dtype):
        max_length = df[column].str.len().max()
        return f"VARCHAR({int(max_length)})"
    else:
        raise ValueError(f"Data type {dtype} is not recognized")

def get_next_versioned_table_name(existing_table_name: str, database_engine: object, date: bool = False) -> str:
    database_inspector = inspect(database_engine)
    all_table_names_in_database = database_inspector.get_table_names()

    existing_versioned_table_names = [table_name for table_name in all_table_names_in_database 
                                      if table_name.startswith(existing_table_name)]

    if not existing_versioned_table_names:
        existing_versioned_table_names = [f"{existing_table_name}_v000"]

    existing_versions = [0]
    version_extraction_pattern = f'{existing_table_name}_v(\d+)'

    for versioned_table_name in existing_versioned_table_names:
        version_match = re.search(version_extraction_pattern, versioned_table_name)
        if version_match is not None:
            existing_versions.append(int(version_match.group(1)))

    next_version_number = max(existing_versions) + 1
    next_version_string = f"v{next_version_number:03d}"

    next_versioned_table_name = f"{existing_table_name}_{next_version_string}"
    # add the date to the table name as 20230101
    if date:
        return next_versioned_table_name + f"_{datetime.now().strftime('%Y%m%d')}"
    return next_versioned_table_name



def write_creation_table_query_string(df: pd.DataFrame, table_name: str) -> str:
    """
    Create a PostgreSQL CREATE TABLE query based on the dtypes of a pandas DataFrame
    """
    query = f"CREATE TABLE IF NOT EXISTS {table_name} (date_loading DATE DEFAULT NOW()::date,"

    for column in df.columns:
        postgres_dtype = pandas_dtype_to_postgres(df, column)
        query += f"\n  {column} {postgres_dtype},"

    query = query.rstrip(",") + "\n);"

    while True:
        answer = input(f"Create table -{table_name}-? (yes/no): ")
        if answer in ["yes", "no", "y", "n"]:
            break
        print("Please answer yes or no.")

    if answer in ["yes", "y"]:
        return query
    else:
        return ''
    
@log_args
def get_newest_folder(data_path: Path, root_folder: str) -> Path:
    """
    Get the newest folder in the data path
    Use the following pattern "{root}{}".format(time.strftime("%Y-%m-%d"))
    """
    if not data_path.is_dir():
        raise NotADirectoryError(f"Path {data_path} is not a directory")
    
    pattern = r".*([\d]{4}-[\d]{2}-[\d]{2})$"
    matching_folders = [folder for folder in data_path.iterdir() if folder.name.startswith(root_folder) and re.match(pattern, folder.name)]
    if not matching_folders:
        return None

    # Sort the folders based on the ctime
    newest_folder = max(matching_folders, key=os.path.getctime)

    return Path(newest_folder)
        

def get_newest_datapath(data_path: Path, re_pattern: str = r".*([\d]{4}-[\d]{2}-[\d]{2}-[\d]{2}-[\d]{2}-[\d]{2})" ) -> Optional[Path]:
    """
    Get the last added data in the data path
    Use the following pattern "2023-07-10-09-51-55"
    """
    if not data_path.is_dir():
        raise NotADirectoryError(f"Path {data_path} is not a directory")

    matching_paths = [folder for folder in data_path.iterdir() if re.match(re_pattern, folder.name)]
    if not matching_paths:
        return None

    # Sort the file paths based on the ctime
    if len(matching_paths) == 1:
        return Path(matching_paths[0])
    else:
        return Path(max(matching_paths, key=os.path.getctime))


def get_matching_folders(data_path: Path, root_folder: str) -> list:
    """
    Get all matching folders in the data path based on the pattern "{root}{}".format(time.strftime("%Y-%m-%d"))
    """
    if not data_path.is_dir():
        raise NotADirectoryError(f"Path {data_path} is not a directory")
    
    pattern = r".*([\d]{4}-[\d]{2}-[\d]{2})$"
    matching_folders = [folder for folder in data_path.iterdir() if folder.name.startswith(root_folder) and re.match(pattern, folder.name)]
    
    return matching_folders

@log_args
def get_newest_csv_path(data_path: Path, root_folder: str = "raw_data_permids_", ) -> pd.DataFrame:
    newestfolder_path = get_newest_folder(data_path=data_path, root_folder=root_folder)
    filepath = get_newest_datapath(data_path=newestfolder_path / "csv")
    return Path(filepath)

if __name__ == "__main__":
    df = pd.read_csv 
    if check_columns_structure(df, ['A', 'B', 'C', 'D', 'E']) and check_types(df, [int, float, str, bool, pd.Timestamp]):
        for column in df.columns:
            print(f"{column}: {pandas_dtype_to_postgres(df, column)}")
        
        print(write_creation_table_query_string(df, "test_postgres_pandas"))
            