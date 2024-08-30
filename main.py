import os
import glob
import datetime
from functions import process_data, export_sheets
from rich.console import Console
import sys

cons = Console(emoji=True, markup=True)
print = cons.print

DATA_FOLDER = os.path.join(os.getcwd(), "copyright_data")
CIP_EXPORT_FOLDER = os.path.join(os.getcwd(), "cipworksheets")
FACULTY_EXPORT_FOLDER = os.path.join(os.getcwd(), "sheets")
ALL_PERIOD_SUFFIXES = ["1A", "1B", "2A", "2B", "3", "SEM1", "SEM2", "JAAR"]


def get_latest_file(path: str) -> str:
    """
    Returns the path to the most recent file in a directory.
    """
    files = glob.glob(os.path.join(path, "*.xlsx"))
    files.sort(key=os.path.getmtime)
    return files[-1]


def get_date(path: str) -> datetime.datetime:
    """
    Returns the date of a file in the format YYYY-MM-DD.
    """
    return datetime.datetime.fromtimestamp(os.path.getmtime(path))


def get_export_path(date: datetime.datetime | None = None, export_folder: str = '') -> str:
    """
    Creates a path to a folder to store the sheets in.
    """
    if date is None:
        export_path = os.path.join(export_folder)
        os.makedirs(export_path, exist_ok=True)
    else:
        export_path = os.path.join(export_folder, date.strftime("%Y-%m-%d"))
        os.makedirs(export_path, exist_ok=True)

    return export_path


def check_if_file_exists(path: str) -> None:
    """
    Checks if a file exists at the given path.
    If it does, it will ask the user if they want to continue.
    If they say no, it will stop the script.
    """
    if os.path.exists(path):
        print(
            f" :warning:  File [magenta]{path}[/magenta] already exists. Do you want to [red]overwrite[/red] it? \nEnter [cyan]y and press enter[/cyan] to continue, any [cyan]other key + enter[cyan] to abort."
        )
        choice = input("> ")
        if choice.lower() != "y":
            print(":warning:  [red]Aborting script[/red].")
            exit()

def error(message: str, e: Exception) -> None:
    '''
    Use this function to throw a semi-informative error if something goes wrong.
    Afterwards, it will ask the user if they want to see the full error message or quit out directly.
    '''
    print(
        f":warning:  [bold red]Error[/bold red] while trying to [bold yellow]{message}[/bold yellow]"
    )
    print("\nPress [magenta]e+enter[/magenta] to show the full error message, or [cyan]any other key+enter[/cyan] to quit.")
    i = cons.input(">  ")
    if i.lower() == "e":
        raise e
    else:
        exit()

def get_file_info(data_folder: str, export_folder:str) -> tuple[str, datetime.datetime, str, str]:
    """
    function to get the latest file in a folder.
    Returns the path, date, export path, and the duckdb filepath.
    """
    try:
        latest_file = get_latest_file(data_folder)
    except Exception as e:
        error(f"get the latest file in the folder {data_folder}. \nIs the folder empty? Did you put an .xlsx file in it?", e)

    try:
        latest_file_date = get_date(latest_file)
    except Exception as e:
        error(f"get the date of the latest file in {data_folder}. \nIs the file empty, corrupt, or not a .xlsx file?", e)

    try:
        export_path = get_export_path(latest_file_date, export_folder)
    except Exception as e:
        error(f"create the export map {latest_file_date.strftime('%Y-%m-%d')} in folder '{export_folder}'. \nIs the folder locked? Do you have the correct permissions?", e)

    db_path = os.path.join(export_path, "data.duckdb")
    check_if_file_exists(db_path)

    return latest_file, latest_file_date, export_path, db_path

def get_latest_export(export_folder: str) -> tuple[str, datetime.datetime]:
    """
    For a given export folder, scan all the foldernames. These should all have the format YYYY-MM-DD.
    Return the path to the folder with the latest date and the date of the folder.
    """
    with os.scandir(os.path.join(os.getcwd(), export_folder)) as mydir:
        dirs = [i.name for i in mydir if i.is_dir()]

    dates = [datetime.datetime.strptime(dir, "%Y-%m-%d") for dir in dirs]
    latest_date = max(dates)
    latest_folder = os.path.join(os.getcwd(), export_folder, latest_date.strftime("%Y-%m-%d"))
    return latest_folder, latest_date

def file_exists(path: str) -> bool:
    """
    Check if a file exists at the given path.
    If it does, return True.
    If it doesn't, return False.
    """
    if os.path.exists(path):
        return True
    else:
        return False

def create_sheets():
    """
    Main function of this script. Should work automatically.
    Will throw a semi-informative error if something goes wrong.
    """
    data_folder = DATA_FOLDER
    export_folder = CIP_EXPORT_FOLDER
    latest_file, latest_file_date, export_path, db_path = get_file_info(data_folder, export_folder)

    periods = [
        str(latest_file_date.year) + "-" + suffix for suffix in ALL_PERIOD_SUFFIXES
    ]

    try:
        process_data(latest_file, periods=periods, dbpath=db_path)
    except Exception as e:
        error(f"process the data. \nDoublecheck that you have the correct permissions to the folder {export_path}, and that the import file {latest_file} contains the correct data.", e)

    try:
        export_sheets(sheets_path=export_path, duckdb_path=db_path, all_faculties=False)
    except Exception as e:
        error(f"export the sheet with all data. \nDoublecheck that you have the correct permissions to the folder {export_path}, and that the import file {latest_file} contains the correct data.", e)

def create_faculty_sheets():
    latest_folder_path, latest_folder_date = get_latest_export(CIP_EXPORT_FOLDER)

    sheet = os.path.join(latest_folder_path,"all","all.xlsx")
    if not file_exists(latest_folder_path):
        error(f'load "all.xlsx" from {latest_folder_path} to create the faculty sheets.', Exception)
    else:
        try:
            export_folder = get_export_path(export_folder=FACULTY_EXPORT_FOLDER)
        except Exception as e:
            error(f"create the export folder '{export_folder}'. \nIs the folder locked? Do you have the correct permissions?", e)
        try:
            export_sheets(sheets_path=export_folder, main_sheet_path=sheet, all_faculties=True, date=latest_folder_date.strftime("%Y-%m-%d"))
        except Exception as e:
            error(f"export sheets for the faculties. \nDoublecheck that you have the correct permissions to the folder {export_folder}, and that the file {sheet} contains the correct data.", e)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "--faculty":
            create_faculty_sheets()
    else:
        create_sheets()


