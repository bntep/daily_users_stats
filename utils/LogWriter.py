#!/usr/bin/env python
"""\
Author: Lucas Boulé, 2023-10-06
Log the arguments of a function and send an email with the log file if an error occurs.
Usage:
@log_args(mail=True, receiver_mail=".....@.....com")
def test_function():
    print("Testing the log_args decorator")
    raise Exception("Exception raise, this is an intended behavior")
"""
import sys
import os
from pathlib import Path
import datetime
import functools
import logging
sys.path.append(str(Path(os.getcwd())))
from utils.Mailer import send_log_mail
from module.env import *

def log_location() -> Path:
    current_file_path = Path(sys.argv[0])
    parent_folder_1 = current_file_path.parent.name
    parent_folder_2 = current_file_path.parent.parent.name
    log_filename = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-') + parent_folder_2 + '-' + parent_folder_1 + '__' + current_file_path.stem + '.log'
    LOG_PATH = Path(os.path.join(os.getcwd(), 'logs', datetime.datetime.now().strftime('%Y_%m_%d'), parent_folder_2, parent_folder_1, log_filename))
    return LOG_PATH
    

# Logging Setup
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
    

def log_location() -> Path:
    current_file_path = Path(sys.argv[0])
    parent_folder_1 = current_file_path.parent.name
    parent_folder_2 = current_file_path.parent.parent.name
    log_filename = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-') + parent_folder_2 + '-' + parent_folder_1 + '__' + current_file_path.stem + '.log'
    LOG_PATH = Path(os.path.join(os.getcwd(), 'logs', datetime.datetime.now().strftime('%Y_%m_%d'), parent_folder_2, parent_folder_1, log_filename))
    
    return LOG_PATH


LOG_PATH = log_location()

# LOG_RESULTS = Path(os.path.join(os.getcwd(), 'resultat/current_file_path.stem'))
# Ensure logs directory exists
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

db_logger = log_config(log_location())

# Write a decorator to log the arguments
def log_args(receiver_email, results_path: Path, mail: bool = False, message_email: str = "", started_at = datetime.datetime.now(), hide_args_in_logs: bool = True, subject_prefix: str = "LOG_DEV"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args_lst = list(args)
            kwargs_dict = dict(kwargs)
            try:
                result = func(*args_lst, **kwargs_dict)
            except Exception as e:
                db_logger.error(f'ERROR: {func.__name__}: {str(e)}')
                if mail is True:
                    # log_path = log_location()
                    send_log_mail(LOG_PATH, results_path, receiver_email, message_email, subject_prefix=subject_prefix)
                    # send_email(subject, html, sender, receiver, logger, rep_file=None, file=log_path)
                raise e
            else:
                if hide_args_in_logs is False:
                   db_logger.info(f'Calling: {func.__name__} with args: {args_lst} and kwargs: {kwargs_dict}')
                else:
                    db_logger.info(f'Calling: {func.__name__}')
                if mail is True:
                    # log_path = log_location()
                    send_log_mail(LOG_PATH, results_path, receiver_email, message_email, subject_prefix=subject_prefix)
                    # send_email(subject, html, sender, receiver, logger, rep_file=None, file=log_path)
                return result
        return wrapper
    return decorator
 

# if __name__ == "__main__":
#     test_function()