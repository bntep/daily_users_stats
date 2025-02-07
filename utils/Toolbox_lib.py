#!/usr/bin/env python
"""\
Author: Lucas Boulé, 2023-10-06
General toolbox for projects.
"""
import re
import traceback
import os
import sys
import subprocess
from pathlib import Path
from typing import Optional, Union, List
import pandas as pd
import glob
from tqdm import tqdm
sys.path.append(str(Path("/home/groups/daily/travail/Bertrand/Developpement/daily_report_extraction")))
from module.env import *


def isInclude(L1, L2):
    """Renvoie True si L2 est incluse dans L1, False sinon"""
    return set(L2) <= set(L1)

def sanitize_pathname(pathname: str, replacement: str = "_", pattern: str = r'[<>:"/\\|?*\0 ]'):
    return re.sub(pattern, replacement, pathname)

def read_csv(filepath, tablename, sep: str = ","):
    if not os.path.exists(filepath):
        raise ValueError(f"Path {filepath} does not exist")
    if not tablename:
        raise ValueError(f"Table name is empty")
    if sep not in [",", ";", "\t"]:
        raise ValueError(f"Separator {sep} is not supported")
    try:
        df = pd.read_csv(filepath, sep=sep, encoding='utf-8', low_memory=True).convert_dtypes().replace({pd.NA: None})
        return tablename, df
    except Exception as e:
        raise ValueError(f"Error reading csv file {filepath}: {e}")
    
def get_last_function_and_line_of_main_script(tb, main_script_path):
    """
    Extract the last function and line number in the traceback from the main script.
    """
    for frame in reversed(tb):
        if os.path.abspath(frame.filename) == main_script_path:
            return frame.name, frame.lineno, frame.filename
    return None, None, None

def safe_cmd(command: str, cwd: Optional[Path] = None) -> tuple:
    cwd = cwd or Path.cwd()

    try:
        # Running the command in the shell
        result = subprocess.run(command, cwd=str(cwd), check=True, text=True, shell=True, capture_output=True)

        # Check for "ERREUR" in the output
        if result.stderr:
            print(f"ERREUR detected in command output, out:{result.stdout}, err:{result.stderr}")

    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        raise
    except OSError as e:
        print(f"Execution failed: {e}")
        raise
    finally:
        print(f"\n\nCommand:\n {command}\n\n")

    return result.stdout, result.stderr

def stream_cmd(command: List[str], cwd: Optional[Path] = None):
    """
    ! NEEDS A LIST OF COMMAND ARGUMENTS !
    Stream the output of a command in the shell and print it in real time.
    This version avoids using `shell=True` for safety and expects a list of command arguments.
    """
    cwd = cwd or Path.cwd()

    try:
        # Starting command without shell=True
        process = subprocess.Popen(command, cwd=str(cwd),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Stream stdout
        while True:
            output_line = process.stdout.readline()
            if not output_line and process.poll() is not None:
                break
            if output_line:
                print(output_line, end='')

       # Stream stderr and check for "ERREUR"
        error_line = process.stderr.readline()
        while error_line:
            print(error_line, end='')  # Print errors as they come
            if "ERREUR" in error_line:
                print(f"ERREUR detected in command output, err: {error_line}")
            error_line = process.stderr.readline()  # Read the next line
            if error_line is None:  # Check if there's more data
                break

    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        raise
    except OSError as e:
        print(f"Execution failed: {e}")
        raise
    finally:
        # Join command list to string for printing
        print(f"\n\nCommand:\n {' '.join(command)}\n\n")

def print_error_with_clickable_path(e, prefix="", main_script_path=None):
    if main_script_path is None:
        # Assuming the script using the utility is the main script.
        main_script_path = os.path.abspath(traceback.extract_stack(limit=2)[-2].filename)

    tb = traceback.extract_tb(e.__traceback__)
    last_function, line_i, file_path = get_last_function_and_line_of_main_script(tb, main_script_path)
    clickable_path = f"{file_path}:{line_i}" if line_i and file_path else None
    print(f"\nError parsing line {line_i}: {prefix} error:{e} in function {last_function}. Click to view: {clickable_path}")


def load_csv_data_to_dataframe(csv_folder_path, tablename:str, pattern: str = '*.csv'):
    if not os.path.exists(csv_folder_path):
        raise ValueError(f"Path {csv_folder_path} does not exist")
    if not isinstance(tablename, str):
        raise ValueError(f"Table name is not a string")
    if not pattern:
        raise ValueError(f"Pattern is empty")

    files = glob.glob(os.path.join(csv_folder_path, pattern))
    if not files:
        return FileNotFoundError(f"No files found in {csv_folder_path} with pattern {pattern}")
    
    file_template=files[0]
    tuple_tablename_dataframe = read_csv(file_template, tablename, sep=",")
    if isinstance(tuple_tablename_dataframe, tuple):
        yield tuple_tablename_dataframe
    else:
        raise ValueError(f"Error reading csv file {file_template}")

"""
Function to create a calendar dataframe
input: date_debut, date_fin, fréquence
ouput: a dataframe 
"""

def create_year_calendar (date_debut, date_fin, freq, daily:bool = True ) -> pd.DataFrame:
    
    #setting up index date range
    if daily :
        start = date_debut.isoformat()
        end = date_fin.isoformat()
        idx = list(pd.bdate_range(start, end, freq = freq, weekmask=WEEKMASK))
    else:
        start = date_debut
        end = date_fin
        idx = list(pd.bdate_range(start, end, freq = freq))
    #create the dataframe using the index above, and creating the empty column for interval_name
    df = pd.DataFrame.from_dict({'date_cotation': idx})
    df['date_cotation'] = pd.to_datetime(df['date_cotation']).dt.date   
    return df


# Function to remove non-printable characters
def remove_non_printable_chars(value: str) -> str:
    if isinstance(value, str):
        return re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
    return value

# Function to create a log
def create_logger (FICHIER_LOG, db : bool = False) ->  object:

    lg.basicConfig(level=lg.DEBUG)

    # Create a custom logger
    logger=lg.getLogger(__name__)
    db_logger = lg.getLogger('sqlalchemy.engine')

    # Create handlers
    c_handler=lg.StreamHandler()
    f_handler=lg.FileHandler(str(FICHIER_LOG))
    db_handler=lg.FileHandler(str(FICHIER_LOG))   

    c_handler.setLevel(lg.DEBUG)
    f_handler.setLevel(lg.INFO)
    db_logger.setLevel(lg.INFO)

    # Create formatters and add it to handlers
    c_format=lg.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format=lg.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    db_format=lg.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    db_handler.setFormatter(db_format)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    db_logger.addHandler(db_handler)

    lg.info("Logging setup complete.")
    
    if db == True:
        return db_logger

    return logger

if __name__ == "__main__":
    try:
        # Some code that raises an exception...
        1/0
    except Exception as e:
        print_error_with_clickable_path(e, prefix="Your custom prefix")