
import os
import glob
import datetime
from functions import process_data, export_sheets
from rich.console import Console

cons = Console(emoji=True, markup=True)
print = cons.print

datafolder='copyright_data'
exportfolder='sheets'

DATA_FOLDER = os.path.join(os.getcwd(), datafolder)
EXPORT_FOLDER = os.path.join(os.getcwd(), exportfolder)
ALL_PERIOD_SUFFIXES = ['1A', '1B', '2A', '2B', '3', 'SEM1', 'SEM2', 'JAAR']

def get_latest_file(path: str) -> str:
    '''
    Returns the path to the most recent file in a directory.
    '''
    files = glob.glob(os.path.join(path, '*.xlsx'))
    files.sort(key=os.path.getmtime)
    return files[-1]

def get_date(path: str) -> datetime.datetime:
    '''
    Returns the date of a file in the format YYYY-MM-DD.
    '''
    return datetime.datetime.fromtimestamp(os.path.getmtime(path))

def get_export_path(date: datetime.datetime) -> str:
    '''
    Creates a path to a folder to store the sheets in.
    '''
    export_path = os.path.join(EXPORT_FOLDER, date.strftime('%Y-%m-%d'))
    os.makedirs(export_path, exist_ok=True)
    return export_path

def check_if_file_exists(path: str) -> None:
    '''
    Checks if a file exists at the given path.
    If it does, it will ask the user if they want to continue.
    If they say no, it will stop the script.
    '''
    if os.path.exists(path):
        print(f' :warning:  File [magenta]{path}[/magenta] already exists. Do you want to [red]overwrite[/red] it? \nEnter [cyan]y and press enter[/cyan] to continue, any [cyan]other key + enter[cyan] to abort.')
        choice = input('> ')
        if choice.lower() != 'y':
            print(':warning:  [red]Aborting script[/red].')
            exit()

def create_sheets():
    '''
    Main function of this script. Should work automatically.
    Will throw a semi-informative error if something goes wrong.
    '''

    try:
        latest_file = get_latest_file(DATA_FOLDER)
    except Exception as e:
        print(f':warning: Error while trying to find the latest copyright datafile in {DATA_FOLDER}: {e}')
        raise e
    
    try:
        latest_file_date = get_date(latest_file)
    except Exception as e:
        print(f':warning: Error while trying to get the date of the latest copyright datafile: {e}')
        raise e
    
    try:
        export_path = get_export_path(latest_file_date)
    except Exception as e:
        print(f':warning: Error while trying to create the export map {latest_file_date.strftime('%Y-%m-%d')} in folder sheets: {e}')
        raise e
    
    db_path = os.path.join(export_path, 'data.duckdb')
    check_if_file_exists(db_path)
    periods = [str(latest_file_date.year)+'-'+suffix for suffix in ALL_PERIOD_SUFFIXES]

    try:    
        process_data(latest_file, periods=periods, dbpath=db_path)
    except Exception as e:
        print(f':warning: Error while trying to process the data: {e}')
        raise e
    
    try:
        export_sheets(db_path, export_path)
    except Exception as e:
        print(f':warning: Error while trying to export the sheets: {e}')
        raise e


if __name__ == '__main__':
    create_sheets()