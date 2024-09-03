import duckdb
import polars as pl
import os
from rich.console import Console
import json
from datetime import datetime
from random import randint
'''
Instead of using all the separate excel sheets as the main archive, use a duckdb sql database to hold the data. Keep the excel sheets for easy viewing; but use the duckdb archive for comparisons and creating new sheets and such.
the primary duckdb sql archive should be 1 file containing all the data -- so include all columns from the excel sheets when adding to the db.
Use different tables inside this db where needed.

If an item is updated, move the current row to the 'old_versions' table, and add a new row with the updated data.
Add a foreign key relation to the created row in 'previous_version' in the new row in the column 'previous_version'.
Make sure this foreign key relation remains intact when the item is updated again.

Beside the columns in the copyRIGHT data, we will add columns 'retrieved_from_qlik', 'last_modified', 'previous_version', 'faculty', 'workflow_status', 'workflow_remarks' to each item.
retrieved_from_qlik should be the date of the qlik export file from the copyRIGHT tool.
last_modified should be the date of the last modification to the item in the database.
old_versions is a many2manyfield with the ids of all the previous versions of the item.
'''

cons = Console(emoji=True, markup=True)
print = cons.print

dept_mapping_path = os.path.join(os.path.dirname(__file__), "department_mapping.json")
DEPARTMENT_MAPPING = json.load(open(dept_mapping_path, encoding='utf-8'))

def read_data(filepath: str) -> pl.DataFrame:
    """
    Given the filepath to a .csv or .xlsx file containing the exported copyRIGHT data,
    returns it as a lazily evaluated polars DataFrame.

    Parameters
    ----------
    filepath : str
        The path to the .csv or .xlsx file containing the EA data.
    rename_columns : bool, optional
        Whether to rename the columns for consistency, by default True.

    Returns
    -------
    pl.DataFrame
        The DataFrame containing the data from the .csv file.
    """
    if filepath.endswith(".csv"):
        result = pl.scan_csv(
            filepath,
            null_values=["-", "", "NA", "None"],
        )
        return result
    elif filepath.endswith(".xlsx"):
        result = pl.read_excel(
            filepath,
            raise_if_empty=True,
        )
        return result
    else:
        raise ValueError("File must be .csv or .xlsx")


def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes the column names in a DataFrame:
        - Replaces spaces with underscores
        - Converts to lowercase
        - replace '#' with 'count_'
        - replace '*' with 'x'

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to normalize the column names in.

    Returns
    -------
    pl.DataFrame
        The DataFrame with normalized column names.
    """
    return df.rename(
        lambda col: col.replace(" ", "_")
        .replace("#", "count_")
        .replace("*", "x")
        .lower()
    )


def write_to_db(df: pl.DataFrame, path: str, table_name: str = "easy_access") -> None:
    """
    Writes a polars DataFrame to a duckdb database.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to write to the database.
    path : str
        The path to the duckdb database. Should point to a .duckdb file.
    """
    if not path.endswith(".duckdb"):
        raise ValueError("Database path must be a .duckdb file")

    # if the table already exists, drop it
    with duckdb.connect(database=path, read_only=False) as con:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    with duckdb.connect(database=path, read_only=False) as con:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df;
            """)

    print(
        f":floppy_disk: Stored the data in :duck: duckdb file [magenta]{path}[/magenta]"
    )


def filter_periods(df: pl.DataFrame, periods: tuple[str, str]) -> pl.DataFrame:
    """
    NOTE: This function is currently superfluos, as the data exported from the copyRIGHT tool is already filtered by period.

    Filters the specified periods from a DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to filter the periods from.
    periods : (str, str)
        The period range to filter on.
        The first element of the tuple should be the start of the period range,
        and the second element should be the end of the period range.
        Each period range should be a string in the format YYYY-PP,
        with YYYY being the ACADEMIC year.
        2023 will be interpreted the academic year 2023-2024.
        PP is the period, one from this list: [1A, 1B, 2A, 2B]

        Example:
            periods = ('2023-1A', '2023-2B')
            this will return all data from the academic year 2023-2024.

    Returns
    -------
    pl.DataFrame
        The DataFrame containing the filtered periods.
    """

    def parse_periods(periods: tuple[str, str] | list[str]) -> set[str]:
        """
        Helper function to parse the periods tuple into a list of period strings.
        The tuple should be in the format (start_period, end_period)
        This function returns a list of all period strings that are relevant to this timeframe.
        Each period range should be a string in the format YYYY-PP,
        with YYYY being the ACADEMIC year -- running from september to august the year after.
        2023 will be interpreted as the academic year 2023-2024, running from september 2023 to august 2024.
        PP is the period, one from this list: [1A, 1B, 2A, 2B]

        Returns a list of strings including:
        -> one string for each period in the period range
            (e.g. '2023-1A', '2023-1B', '2023-2A', '2023-2B')
        -> If a period range covers a semester (e.g. 2023-1A, 2023-1B),
            include that semester as well (e.g. '2023-SEM1')
        -> if a period range covers a full year (e.g. 2023-1A, 2023-2B),
            include that year as well (e.g. '2023-JAAR')
        -> if the period range includes a final period (-2B, e.g. 2023-2B),
            include the '3' period as well (e.g. '2023-3')

        For reference, the complete list of named periods in an academic year:
        1A: First period (10 weeks) of the first semester of the academic year
        1B: Second period (10 weeks) of the first semester of the academic year
        2A: First period (10 weeks) of the second semester of the academic year
        2B: Second period (10 weeks) of the second semester of the academic year
        3: Third period (10 weeks) of the academic year (summer term) Not often used; include if any 2B period is included in the request.
        SEM1: First semester of the academic year
        SEM2: Second semester of the academic year
        JAAR: Full academic year
        """
        period_mapping = {
            "1A": 1,
            "1B": 2,
            "2A": 3,
            "2B": 4,
            "3": 5,
        }
        reverse_mapping = {v: k for k, v in period_mapping.items()}

        start_period, end_period = periods
        start_year, start_period_str = start_period.split("-")
        end_year, end_period_str = end_period.split("-")

        if (
            start_period_str not in period_mapping
            or end_period_str not in period_mapping
        ):
            raise ValueError("Invalid period format")

        start_period_number = period_mapping[start_period_str]
        end_period_number = period_mapping[end_period_str]

        period_list = set()

        for year in range(int(start_year), int(end_year) + 1):
            start = start_period_number if year == int(start_year) else 1
            end = end_period_number if year == int(end_year) else 5

            for i in range(start, end + 1):
                period_list.add(f"{year}-{reverse_mapping[i]}")

            if start <= 2 and end >= 2:
                period_list.add(f"{year}-SEM1")
            if start <= 4 and end >= 4:
                period_list.add(f"{year}-SEM2")
            if start <= 2 and end >= 4:
                period_list.add(f"{year}-JAAR")
            if end >= 4:
                period_list.add(f"{year}-3")

        return period_list

    if isinstance(periods, list):
        period_list = periods
    elif isinstance(periods, tuple):
        period_list = list(parse_periods(periods))
    else:
        raise ValueError("Periods must be a list or tuple")
    return df.filter(pl.col("period").is_in(period_list))


def add_faculty_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    map each row in the DataFrame to the corresponding faculty, and add
    this to the faculty column in the DataFrame.

    The mapping is included as a hardcoded dictionary in the function for now.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to add the faculty to.

    Returns
    -------
    pl.DataFrame
        The DataFrame with the faculty column added.
    """



    return df.with_columns(
        faculty=pl.col("department").replace_strict(
            DEPARTMENT_MAPPING, default="Unmapped"
        )
    )

def add_status_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    add the 'status' and 'status_info' columns to the dataframe.
    These should be used by the faculties to indicate the status of each item.
    'status' is initially set to 'not checked', and 'status_info' is set to '-'.

    TODO: get a list of possible workflow statuses from the SURF discussions

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to add the status columns to.

    Returns
    -------
    pl.DataFrame
        The DataFrame with the status columns added.
    """

    return df.with_columns([
        pl.lit('not checked').alias('workflow_status'),
        pl.lit('-').alias('workflow_remarks')
    ])


def process_data(filepath: str, periods: tuple[str, str] | list[str], dbpath: str):
    """
    # NOTE: Currently 'periods' is not used, as the exported data is already filtered by period.
    Main function: supply a filepath to a .csv or .xlsx file containing the EA data,
    and a period tuple to filter the data by.

    This script will then read, process, and filter the data, and write it to the duckdb database.

    Parameters
    ----------
    filepath : str
        The path to the .csv or .xlsx file containing the EA data.
    periods : tuple[str, str] | list[str]
        A tuple with a period range, or a list of periods to filter on.

        Each period should be a string in the format YYYY-PP,
        with YYYY being the ACADEMIC year.
        2023 will be interpreted the academic year 2023-2024.
        PP is the period, one from this list: [1A, 1B, 2A, 2B]

        When using tuples:
        The first element of the tuple should be the start of the period range,
        and the second element should be the end of the period range.

        When using lists:
        Each element should be a string in the format YYYY-PP,
        with YYYY being the ACADEMIC year. In list form, include all periods you want to include, not just the period range.

    dbpath : str
        The duckdb file with the processed data will be stored here.
    """

    '''
    # including filter_periods
    write_to_db(
        add_faculty_column(
            filter_periods(normalize_column_names(read_data(filepath)), periods)
        ),
        path=dbpath,
    )
    '''

    write_to_db(
        add_faculty_column(
            normalize_column_names(read_data(filepath))
        ),
        path=dbpath,
    )

def export_single_sheet(duckdb_path: str, sheets_path: str) -> None:
    """
    Same as export_sheets, but only exports a single sheet with all data.
    """
    export_sheets(duckdb_path, sheets_path, all_faculties=False)

def prepare_faculty_sheets(df: pl.DataFrame, sheets_path: str, date: str|None = None) -> list[tuple[str, pl.DataFrame]]:
    """
    From a dataframe with the living cip worksheet data
    create a list of tuples with the path to store the .xlsx, and the dataframe with that faculty's data.
    """
    df = add_status_columns(df)
    faculties = df.select(pl.col("faculty").unique())
    faculties = faculties.to_series().to_list()
    resultlist: list[tuple[str, pl.DataFrame]] = []
    for faculty in faculties:
        if faculty is None:
            faculty = "no_faculty_found"
            df_faculty = df.filter(pl.col("faculty").is_null())
        else:
            df_faculty = df.filter(pl.col("faculty") == faculty)

        if faculty == "":
            faculty = "no_faculty_found"
        sheet_name = str(faculty) + ".xlsx"

        if not date:
            fac_folder_path = os.path.join(sheets_path, faculty)
        else:
            fac_folder_path = os.path.join(sheets_path, faculty, date)
        # check if path exists, if not, create it
        if not os.path.exists(fac_folder_path):
            os.makedirs(fac_folder_path, exist_ok=True)

        fac_sheet_path = os.path.join(fac_folder_path, sheet_name)
        resultlist.append((fac_sheet_path, df_faculty))
        print(
            f":floppy_disk: saved [cyan]{faculty}[/cyan] data to :clipboard: [magenta]{fac_sheet_path}[/magenta]"
        )
    return resultlist

def export_sheets(sheets_path: str, duckdb_path: str|None = None, main_sheet_path: str|None = None, all_faculties: bool = True, date: str|None = None) -> None:
    """
    Read in a duckdb file and a path to store the output.
    Then create a sheet for each faculty, and one with all data, and store it in that new map.

    Parameters
    ----------
    duckdb_path : str
        The path to the duckdb file containing the processed data.
    sheets_path : str
        The path to the folder where the sheets should be stored.
    """
    if duckdb_path:
        with duckdb.connect(database=duckdb_path, read_only=True) as con:
            df: pl.DataFrame = con.execute("SELECT * FROM easy_access").pl()
    elif main_sheet_path:
        df = pl.read_excel(main_sheet_path, raise_if_empty=True)

    # add the date to column 'export_date'. If no date is given, use today's date.

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    df = df.with_columns(pl.lit(date).alias('export_date'))


    if all_faculties:
        faculty_sheets = prepare_faculty_sheets(df, sheets_path, date)
        for faculty_sheet in faculty_sheets:
            faculty_sheet_path = faculty_sheet[0]
            df_faculty = faculty_sheet[1]
            df_faculty.write_excel(faculty_sheet_path)

    # check if sheets_path\all.xlsx exists. If it does, raise error.
    sheet_name = 'all.xlsx'
    if date:
        all_folder_path = os.path.join(sheets_path, 'all', date)
    else:
        all_folder_path = os.path.join(sheets_path, 'all')

    if not os.path.exists(all_folder_path):
        os.makedirs(all_folder_path, exist_ok=True)
    all_sheet_path = os.path.join(all_folder_path, sheet_name)

    if os.path.exists(all_sheet_path):
        raise ValueError(f"File {all_sheet_path} already exists. Please delete it or rename it before running this script again.")
    else:
        df.write_excel(all_sheet_path)
        print(
            f':floppy_disk: saved the [cyan]full list[/cyan] to :clipboard: [magenta]{all_sheet_path}[/magenta]'
        )

def update_sheet(sheet_path: str, new_data_path: str|pl.DataFrame, print_diffs: bool = False) -> None:
    """
    Update a 'living' worksheet with data from a new qlik export file.
    The newly discovered items will be appended to the current worksheet (an .xlsx file). A backup will be made before adding the new data.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # the added random number is a kludge to avoid overwriting existing backups
    # replace with uuid (a variant that's also time-based)
    backup_path = sheet_path.replace(".xlsx", f"_backup_{today}+{randint(1111111111111111,9999999999999999)}.xlsx")

    # find the current living worksheet
    try:
        cur_sheet = pl.read_excel(sheet_path, raise_if_empty=False)
        # store a backup
        cur_sheet.write_excel(backup_path)
        print('written backup to', backup_path)
    except FileNotFoundError:
        print(f"Sheet {sheet_path} does not exist yet. Creating a new one.")
        cur_sheet = pl.DataFrame()


    # check if backup was properly made
    if not os.path.exists(backup_path):
        raise ValueError(f"Backup was not made, stopping script. Please check that the backup file {backup_path} exists.")

    new_data = pl.DataFrame()
    print(new_data_path)
    if isinstance(new_data_path, str):
        try:
            new_data = pl.read_excel(new_data_path, raise_if_empty=True)
        except pl.exceptions.NoDataError:
            print(f"Empty file found in {new_data_path}. No changes were made.")
            return
    elif isinstance(new_data_path, pl.DataFrame):
        new_data = new_data_path

    # add the column 'added_to_sheet_on' to new data, containing today's date
    new_data = new_data.with_columns(pl.lit(today).alias('added_to_sheet_on'))

    print(cur_sheet.head())
    print(cur_sheet.schema)
    print(new_data.head())
    print(new_data.schema)

    # set all columns with type 'Null' to 'String'
    new_data = new_data.with_columns([pl.when(pl.col(col).is_null()).then(pl.lit("")).otherwise(pl.col(col)).alias(col) for col in new_data.columns])
    cur_sheet = cur_sheet.with_columns([pl.when(pl.col(col).is_null()).then(pl.lit("")).otherwise(pl.col(col)).alias(col) for col in cur_sheet.columns])

    #new items: all rows in new_data that have a material_id not in cur_sheet
    new_items = new_data.filter(~pl.col("material_id").is_in(cur_sheet["material_id"]))
    print(new_items.head())
    print(new_items.schema)

    new_sheet = pl.concat([cur_sheet, new_items], how="vertical")

    # check if merged as the same number of rows as cur_sheet
    if len(new_sheet) == len(cur_sheet):
        print(f"No new items found while adding to {sheet_path}. New data has {len(new_data)}, the old data has {len(cur_sheet)} rows.")
        print("No changes were made.")
        return
    else:
        new_sheet.write_excel(sheet_path)
        print(f"Updated {sheet_path} with {len(new_sheet)-len(cur_sheet)} new items.")
        print("note: writing is temporarily disabled for testing")

    # Compare cur_sheet and new_data. For each matching 'material_id', check if any of the other columns have changed. If so, print the differences.
    common_ids = cur_sheet.select("material_id").filter(pl.col("material_id").is_in(new_data["material_id"]))

    # Compare the corresponding rows and print any differences
    for material_id in common_ids["material_id"]:
        cur_row = cur_sheet.filter(pl.col("material_id") == material_id).to_dict(as_series=False)
        new_row = new_data.filter(pl.col("material_id") == material_id).to_dict(as_series=False)

        differences = {key: (cur_row[key], new_row[key]) for key in cur_row if cur_row[key] != new_row[key]}

        if differences:
            print(f"Differences for material_id {material_id}:")
            print(differences)
            print('\n')


def update_faculty_sheets(cip_worksheet_path: str, faculty_worksheets_base_path: str, print_diffs: bool = False) -> None:
    """
    Creates a backup of the current living faculty worksheets, and then updates them with new data.
    Mainly a wrapper around update_sheet.
    """

    date = datetime.now().strftime("%Y-%m-%d")
    df = pl.read_excel(cip_worksheet_path, raise_if_empty=True)
    new_faculty_worksheets = prepare_faculty_sheets(df, faculty_worksheets_base_path, date)
    for sheet in new_faculty_worksheets:
        update_sheet(sheet_path=sheet[0], new_data_path=sheet[1], print_diffs=print_diffs)


