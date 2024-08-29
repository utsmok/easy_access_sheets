import duckdb
import polars as pl
import os
from rich.console import Console

cons = Console(emoji=True, markup=True)
print = cons.print
def read_data(filepath: str) -> pl.DataFrame:
    '''
    Given the filepath to a .csv or .xlsx file containing the EA data,
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
    '''
    if filepath.endswith('.csv'):
        result = pl.scan_csv(
                            filepath,
                            null_values=['-', '', 'NA', 'None'],
                        )
        return result
    elif filepath.endswith('.xlsx'):
        result = pl.read_excel(
                            filepath,
                            raise_if_empty=True,
                        )
        return result
    else:
        raise ValueError('File must be .csv or .xlsx')

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    '''
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
    '''
    return df.rename(lambda col: col.replace(' ', '_').replace('#', 'count_').replace('*', 'x').lower())

def write_to_db(df: pl.DataFrame, path: str, table_name: str ='easy_access') -> None:
    '''
    Writes a polars DataFrame to a duckdb database.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to write to the database.
    path : str
        The path to the duckdb database. Should point to a .duckdb file.
    '''
    if not path.endswith('.duckdb'):
        raise ValueError('Database path must be a .duckdb file')

    # if the table already exists, drop it
    with duckdb.connect(database=path, read_only=False) as con:
        con.execute(f'DROP TABLE IF EXISTS {table_name}')

    with duckdb.connect(database=path, read_only=False) as con:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df;
            """
        )

    print(f':floppy_disk: Stored the data in :duck: duckdb file [magenta]{path}[/magenta]')

def filter_periods(df: pl.DataFrame, periods: tuple[str, str]) -> pl.DataFrame:
    '''
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
    '''

    def parse_periods(periods: tuple[str, str] | list[str]) -> set[str]:
        '''
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
        '''
        period_mapping = {
            '1A': 1,
            '1B': 2,
            '2A': 3,
            '2B': 4,
            '3': 5,
        }
        reverse_mapping = {v: k for k, v in period_mapping.items()}

        start_period, end_period = periods
        start_year, start_period_str = start_period.split('-')
        end_year, end_period_str = end_period.split('-')

        if start_period_str not in period_mapping or end_period_str not in period_mapping:
            raise ValueError("Invalid period format")

        start_period_number = period_mapping[start_period_str]
        end_period_number = period_mapping[end_period_str]

        period_list = set()

        for year in range(int(start_year), int(end_year) + 1):
            start = start_period_number if year == int(start_year) else 1
            end = end_period_number if year == int(end_year) else 5

            for i in range(start, end + 1):
                period_list.add(f'{year}-{reverse_mapping[i]}')

            if start <= 2 and end >= 2:
                period_list.add(f'{year}-SEM1')
            if start <= 4 and end >= 4:
                period_list.add(f'{year}-SEM2')
            if start <= 2 and end >= 4:
                period_list.add(f'{year}-JAAR')
            if end >= 4:
                period_list.add(f'{year}-3')

        return period_list

    if isinstance(periods, list):
        period_list = periods
    elif isinstance(periods, tuple):
        period_list = list(parse_periods(periods))
    else:
        raise ValueError('Periods must be a list or tuple')
    return df.filter(pl.col('period').is_in(period_list))

def add_faculty_column(df: pl.DataFrame) -> pl.DataFrame:
    '''
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
    '''

    DEPARTMENT_MAPPING: dict[str, str] = {
        'M-ME: Mechanical Engineering':'ET',
        'M-TM: Technical Medicine':'TNW',
        'M-EE: Electrical Engineering':'EEMCS',
        'B-COM: Communication Science':'BMS',
        'M-ES: European Studies':'BMS',
        'M-CS: Computer Science':'EEMCS',
        'M-CSE: Chemical Science & Engineering':'TNW',
        'M-PSY: Psychology':'BMS',
        'M-ITECH: Interaction Technology':'EEMCS',
        'M-ROB: Robotics':'EEMCS',
        'M-BIT: Business Information Technology':'EEMCS',
        'M-IEM: Industrial Engineering and Management':'BMS',
        'M-AM: Applied Mathematics':'EEMCS',
        'M-EEM: Environmental and Energy Management':'BMS',
        'B-AT: Advanced Technology':'TNW',
        'M-BME: Biomedical Engineering':'TNW',
        'M-NT: Nanotechnology':'TNW',
        'M-CEM: Civil Engineering and Management':'ET',
        'BMS: Behavioural, Management and Social Sciences':'BMS',
        'B-AM: Applied Mathematics':'EEMCS',
        'M-AP: Applied Physics':'TNW',
        'M-GEO-WO: Geo-information Science and Earth Observation':'ITC',
        'B-CSE: Chemical Engineering':'TNW',
        'B-ATLAS: Technology and Liberal Arts & Sciences':'ITC',
        'M-EMSYS: Embedded Systems':'EEMCS',
        'M-IDE: Industrial Design Engineering':'ET',
        'B-PSY: Psychology':'BMS',
        'M-EST: Educational Science and Technology':'BMS',
        'B-EE: Electrical Engineering':'EEMCS',
        'M-PSTS: Philosophy of Science, Technology and Society':'BMS',
        'Master Risicomanagement':'',
        'EEMCS: Electrical Engineering, Mathematics and Computer Science':'EEMCS',
        'M-CME: Construction Management and Engineering':'ET',
        'M-BA: Business Administration':'BMS',
        'M-SE: Spatial Engineering':'ITC',
        'B-TCS: Technical Computer Science':'EEMCS',
        'B-CREA: Creative Technology':'EEMCS',
        'B-CE: Civil Engineering':'ET',
        'B-TG: Technische Geneeskunde':'TNW',
        'B-IBA: International Business Administration':'BMS',
        'B-BMT: Biomedische Technologie':'TNW',
        'B-GZW: Gezondheidswetenschappen':'TNW',
        'B-MST: Management, Society and Technology':'BMS',
        'B-IEM: Industrial Engineering and Management':'BMS',
        'B-IDE: Industrial Design Engineering':'ET',
        'B-BIT: Business Information Technology':'EEMCS',
        'B-ME: Mechanical Engineering':'ET',
        'B-TN: Technische Natuurkunde':'TNW',
        'M-COM: Communication Science':'BMS',
        'B-ME-VU: Mechanical Engineering - Amsterdam (VU-UT)':'ET',
        'Testcourses':'',
        'M-SET: Sustainable Energy Technology':'ET',
    }

    return df.with_columns(
        faculty=pl.col('department').replace_strict(DEPARTMENT_MAPPING, default='Unmapped')
    )

def process_data(filepath: str, periods: tuple[str, str] | list[str], dbpath: str):
    '''
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
    '''

    # this is a pretty dumb implemention; rewrite to better use polars syntax?

    write_to_db(
        add_faculty_column(
            filter_periods(
                normalize_column_names(
                    read_data(filepath)
                ),
                periods
            )
        ), path=dbpath
    )

def export_sheets(duckdb_path: str, sheets_path: str):
    '''
    Read in a duckdb file and a path to store the output. 
    Then create a sheet for each faculty, and one with all data, and store it in that new map.

    Parameters
    ----------
    duckdb_path : str
        The path to the duckdb file containing the processed data.
    sheets_path : str
        The path to the folder where the sheets should be stored.
    '''
    with duckdb.connect(database=duckdb_path, read_only=True) as con:
        df: pl.DataFrame = con.execute('SELECT * FROM easy_access').pl()
    
    faculties = df.select(pl.col('faculty').unique())
    faculties = faculties.to_series().to_list()

    for faculty in faculties:
        
        df_faculty = df.filter(pl.col('faculty') == faculty)
        if faculty == '':
            faculty = 'no_faculty_found'
        sheet_name = str(faculty)+'.xlsx'
        fac_sheet_path = os.path.join(sheets_path, sheet_name)
        print(f':floppy_disk: saved [cyan]{faculty}[/cyan] data to :clipboard: [magenta]{fac_sheet_path}[/magenta]')
        df_faculty.write_excel(fac_sheet_path)
    
    df.write_excel(os.path.join(sheets_path, 'all.xlsx'))
    print(f':floppy_disk: saved the [cyan]full list[/cyan] to :clipboard: [magenta]{os.path.join(sheets_path, "all.xlsx")}[/magenta]')


if __name__ == '__main__':
    # testing code
    process_data(
        filepath='sources/easy_access/export_full_13_08_2024.xlsx',
        periods=(['2024-1A', '2024-JAAR', '2024-SEM1'])
    )