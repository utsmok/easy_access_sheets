import duckdb
import polars as pl
import dotenv
from rich.console import Console
import datetime
import uuid
import os
import json
import pathlib
import shutil
import ibis

print = Console(emoji=True, markup=True).print
dotenv.load_dotenv('settings.env')

class Directory:
    '''
    Simple class for directories + operations.
    Init with an absolute path, or a path relative to the current working directory.
    If the dir does not yet exist, it will be created. Disable this by setting the 'create_dir' parameter to False.
    '''
    def __init__(self, path: str, create_dir: bool = True):
        self.input_path_str = path
        self.create_dir = create_dir

        # check if the path is absolute
        if pathlib.Path(path).is_absolute():
            self.full = pathlib.Path(path)
        else:
            self.full = pathlib.Path.cwd() / path

        self.post_init()

    def post_init(self) -> None:
        '''
        Checks to see if this is actually a dir,
        or create it if create_dir is set to True.
        '''
        if not self.full.exists():
            if self.create_dir:
                self.create()
            else:
                raise FileNotFoundError(f"Directory {self.full} does not exist and create_dir is set to False.")
        if not self.full.is_dir():
            raise NotADirectoryError(f"Directory {self.full} is not a directory.")

    def files(self) -> list['File']:
        '''
        Returns all files in the dir as a list of File objects.
        '''
        return [File(self.full / file) for file in self.full.iterdir() if file.is_file()]

    def dirs(self, r: bool = False) -> list['Directory']:
        '''
        Returns a list of all dirs in this Directory as a list of Directory objects.
        If r is set to True, it will return all children dirs recursively.
        '''
        if not r:
            return [Directory(self, d) for d in self.full.iterdir() if d.is_dir()]
        if r:
            return [Directory(self, d) for d in self.full.rglob('*') if d.is_dir()]

    def exists(self) -> bool:
        return self.full.exists()

    def is_dir(self) -> bool:
        return self.full.is_dir()

    def create(self) -> None:
        try:
            self.full.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass

    def __eq__(self, other) -> bool:
        return self.full == other.full

    def __str__(self):
        return str(self.full)

    def __repr__(self):
        return f"DirPath('{self.input_path_str}') -> {self.full}"

class File:
    '''
    Simple class for files + operations
    Parameters:
        path: str or Path
            relative from the current working directory.
            OR
            absolute path to the file.
            Should always end with the filename including extension.
    '''
    def __init__(self, path: str|pathlib.Path):

        self.path_init_str = str(path)

        assert isinstance(path, str) or isinstance(path, pathlib.Path)

        if isinstance(path, pathlib.Path):
            self.path = path
            self.name = path.name
            self.extension = path.suffix
            self.dir = Directory(str(self.path.absolute().parent))

        elif isinstance(path, str):
            if '/' in path:
                self.name = path.rsplit('/',1)[-1]
                self.dir = Directory(path.rsplit('/', 1)[0], create_dir=True)
            else:
                self.name = path
                self.dir = Directory(os.getcwd())

            self.extension = self.name.split('.')[-1]
            self.path = self.dir.full / self.name


    def move(self, new_dir: str) -> None:
        new_dir: Directory = Directory(new_dir, create_dir=True)
        self.path.rename(new_dir.full / self.name)

    def rename(self, new_name: str) -> None:
        self.path.rename(self.dir.full / new_name)

    def copy(self, new_path: str) -> None:
        if '.' in new_path:
            # includes file name
            copy_name = new_path.rsplit('/', 1)[-1]
            copy_dir = Directory(new_path.rsplit('/', 1)[0], create_dir=True)
        else:
            # no file name, so use the same name
            copy_name = self.name
            copy_dir = Directory(new_path, create_dir=True)

        source = self.path
        destination = copy_dir.full / copy_name
        shutil.copy(source, destination)

    def created(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.path.stat().st_birthtime)

    def modified(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.path.stat().st_mtime)

    def exists(self) -> bool:
        return self.path.exists()

    def is_file(self) -> bool:
        return self.path.is_file()

    def __eq__(self, other) -> bool:
        return self.path == other.path

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return f"File('{self.path_init_str}') -> {self.path}"

class Sheet:
    """
    Manipulate/read/write/copy/compare/update a single worksheet.
    """

    def __init__(self, worksheet_file: File, sheet_type: str):
        """
        Initialize the class with the path to the worksheet file.
        sheet_type should be 'all' or one of the faculty abbreviations (e.g. 'TNW', 'ITC', etc).
        """
        self.file = worksheet_file
        self.path = worksheet_file.path
        try:
            self.current_sheet_data: pl.DataFrame = pl.read_excel(self.path, raise_if_empty=True)
        except FileNotFoundError:
            print(f"File {worksheet_file} does not exist. Creating a new one.")
            self.current_sheet_data = pl.DataFrame()
        self.archive: Archive = Archive()
        self.sheet_type: str = sheet_type

    def update(self) -> None:
        """
        Takes the worksheet, compares the data to what's in duckdb for this faculty (or 'all' if it's the main worksheet), and updates the worksheet with the new data.
        """

        if self.sheet_type == 'all':
            archive_items = self.archive.get()
        else:
            archive_items = self.archive.get(search_terms=[('faculty',str(self.sheet_type))])

        if self.current_sheet_data.is_empty():
            new_items = archive_items
        else:
            new_items = archive_items.filter(~pl.col("material_id").is_in(self.current_sheet_data["material_id"]))

        # add column 'added_to_sheet_on' to new data, containing today's date
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        new_items = new_items.with_columns(pl.lit(today).alias('added_to_sheet_on'))
        if not new_items.is_empty():
            new_items = new_items.with_columns([pl.when(pl.col(col).is_null()).then(pl.lit("")).otherwise(pl.col(col)).alias(col) for col in new_items.columns])
            self.current_sheet_data = self.current_sheet_data.with_columns([pl.when(pl.col(col).is_null()).then(pl.lit("")).otherwise(pl.col(col)).alias(col) for col in self.current_sheet_data.columns])
            self.new_sheet_data = pl.concat([self.current_sheet_data, new_items], how="vertical")
            self.save()
        else:
            print(f"No new items to add to the archive for {self.sheet_type}.")
            self.new_sheet_data = pl.DataFrame()

    def compare(self) -> None:
        """
        for each row, get the differences between the current worksheet and the same rows in the archive
        """
        all_ids = self.current_sheet_data.select(pl.col("material_id")).unique().to_dicts()
        archive_data = self.archive.get(search_terms=all_ids)

        common_ids = self.current_sheet_data.select("material_id").filter(pl.col("material_id").is_in(archive_data["material_id"]))
        for material_id in common_ids["material_id"]:
            cur_row = self.current_sheet_data.filter(pl.col("material_id") == material_id).to_dict(as_series=False)
            new_row = archive_data.filter(pl.col("material_id") == material_id).to_dict(as_series=False)
            differences = {key: (cur_row[key], new_row[key]) for key in cur_row if cur_row[key] != new_row[key]}

            if differences:
                print(f"Differences for material_id {material_id}:")
                print(differences)
                print('\n')


    def save(self) -> None:
        if self.new_sheet_data.is_empty():
            print(f"No new items to add to the archive for {self.sheet_type}.")
            return
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        unique_id = uuid.uuid1()
        if self.path.exists():
            backup_path = self.path.replace(f"{self.path.parent / self.path.stem}_backup_{today}_{unique_id}.xlsx")
            if not backup_path.exists():
                raise ValueError(f"Backup was not made, stopping script. Please check that the backup file {backup_path} exists.")
        self.new_sheet_data.write_excel(self.path)

    def store_final_data(self) -> None:
        """
        read in the data from the sheet and update the duckdb table 'final_data' with it.
        """
        self.archive.store_final_data(self.current_sheet_data)

class CopyRightData:
    def __init__(self, qlik_export_file: str | File, dept_mapping_path: str | File | None = None):

        if isinstance(qlik_export_file, str):
            self.qlik_export_file = File(qlik_export_file)
        else:
            self.qlik_export_file = qlik_export_file

        if not dept_mapping_path:
            dept_mapping_path = File("department_mapping.json")
        if isinstance(dept_mapping_path, str):
            dept_mapping_path = File(dept_mapping_path)

        self.DEPARTMENT_MAPPING = json.load(open(dept_mapping_path.path, encoding='utf-8'))
        self.data = self.to_df()
        self.data = self.clean()
        self.data = self.add_faculty_column()
        self.data = self.process()
        self.archive = Archive()


    def to_df(self) -> pl.DataFrame:
        """
        imports data from the Qlik export file into a Polars dataframe
        """
        return pl.read_excel(self.qlik_export_file.path)

    def clean(self) -> pl.DataFrame:
        """
        cleans and preprocesses the imported data
        """

        return self.data.rename(
            lambda col: col.replace(" ", "_")
            .replace("#", "count_")
            .replace("*", "x")
            .lower()
            )

    def add_faculty_column(self) -> pl.DataFrame:
        return self.data.with_columns(
            faculty=pl.col("department").replace_strict(
                self.DEPARTMENT_MAPPING, default="Unmapped"
            )
        )
    
    def process(self) -> pl.DataFrame:
        """
        adds the extra columns (e.g. faculty, workflow_status, workflow_remarks, ...), checks for errors, adds 'retrieved_from_qlik' date, etc
        """
        qlik_file_created_date: str = self.qlik_export_file.created().strftime("%Y-%m-%d")
        return self.data.with_columns(
            pl.Series("retrieved_from_qlik", [qlik_file_created_date] * len(self.data)),
            pl.Series("workflow_status", ["not checked"] * len(self.data)),
            pl.Series("workflow_remarks", ["-"] * len(self.data)),
        )

    def store(self) -> None:
        self.archive.add(self.data)

class Archive:
    '''
    This class handles all duckdb interactions.
    It stores the archive of all the data.
    Currently this is just a list of the qlik data as it was initially imported.
    
    '''
    def __init__(self, db_path: str|File|None = None):
        if not db_path:
            self.db_path = File(os.getenv("DUCKDB_PATH"))
            if not self.db_path:
                raise ValueError("No database path provided and no DUCKDB_PATH environment variable found.")
        else:
            if isinstance(db_path, str):
                self.db_path = File(db_path)
            else:
                self.db_path = db_path
        self.con =  ibis.connect(f'duckdb://{self.db_path.name}')
        if 'archive' in self.con.list_tables():
            self.archive = self.con.table('archive')
        else:
            self.archive = None
        
        #self.print_archive_columns()

    def print_archive_columns(self) -> None:
        ibis.options.interactive = True
        print(self.archive.head())
        ibis.options.interactive = False

    def add(self, df: pl.DataFrame):
        '''
        takes in a dataframe from the DataImporter, and adds any rows not currently in the archive. If no archive exists, create it.
        Make sure to check all the fields with dates like 'last_modified' and 'retrieved_from_qlik' are correct.
        If rows already exist, ignore them (keep the old ones).
        '''

        # first check the df for errors
        df = self.check_dataframe(df)
        if self.archive is None:
            with duckdb.connect(database='archive.duckdb', read_only=False) as con:
                con.execute("""
                    CREATE TABLE 'archive' AS SELECT * FROM df;
                    """)
            self.archive = self.con.table('archive')
        else:
            # if archive exists, add only rows which are not already in the archive
            # check this by comparing the material_id column
            existing_material_ids = self.archive.select('material_id').to_polars()
            new_material_ids = df.filter(~df['material_id'].is_in(existing_material_ids['material_id']))
            if not new_material_ids.is_empty():
                new_material_ids = ibis.memtable(new_material_ids)
                self.archive = self.archive.union(new_material_ids)
            else:
                print("No new rows to add to the archive.")

    def store_final_data(self, df: pl.DataFrame) -> None:
        '''
        Store the data from the worksheet in the final_data table in the duckdb archive.
        '''

    def update(self, df: pl.DataFrame):
        '''
        update existing rows in the archive with new data from qlik or worksheets. Make sure to handle old_versions column correctly, as well as all other dates.
        '''

    def get(self, search_terms: list[tuple[str, str]]|None = None) -> pl.DataFrame:
        '''
        get specific rows from the archive
        search_terms should be a list with dictionaries with shape {'field_name': 'value'}
        if search_terms is None, return all rows
        '''

        if search_terms is None:
            return self.archive.to_polars()
        else:
            #TODO: test this!!
            # return all rows from self.archive where all conditions in the search terms are met
            # search terms are a list of tuples, where the first element is the column name, and the second element is the value to match
            # e.g. [('material_id', '12345'), ('workflow_status', 'not checked')]
            filtered = self.archive
            for key, value in search_terms:
                filtered = filtered.filter(filtered[key] == value)
            return filtered.to_polars()
    def check_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        '''
        check a dataframe for errors & see if it has the correct columns
        use this before inserting data into the archive
        currently not implemented.
        '''
        return df

class EasyAccess:
    ...
    '''
    class EasyAccess
    description: Main class to handle the entire workflow.
    parameters: none (?)
    methods:
    process_new_data: grabs the  CopyRightData file with the latest 'created' date from dir os.getenv(QLIK_EXPORTS_DIR).
    Process the data, insert it into the archive.
    Then update all the sheets: all.xlsx with every item, and one for each faculty. Use archive.get() to get all the data. Use this to get the list of unique faculties for further processing.
    '''

    def __init__(self):
        self.archive = Archive()

        self.copyright_data_dir = Directory(os.getenv("QLIK_EXPORTS_DIR"))
        self.faculty_sheet_dir = Directory(os.getenv("FACULTY_SHEETS_DIR"))
        self.cip_worksheet_path = File(os.path.join(os.getenv("CIP_WORKSHEET_DIR"), "all.xlsx"))
        self.copyright_data, self.previous_copyright_data = self.get_latest_export()
        self.sheets: list[Sheet] = []

    def get_latest_export(self) -> tuple[CopyRightData, CopyRightData]:
        """
        Scan all files in self.copyright_data_dir and return the 2 latest ones.
        Use the created date to determine which file to return.
        """
        all_files = self.copyright_data_dir.files()
        latest_file = max(all_files, key=lambda x: x.created())
        previous_file = max(all_files, key=lambda x: x.created(), default=latest_file)
        return CopyRightData(qlik_export_file=latest_file), CopyRightData(qlik_export_file=previous_file)

    def run(self) -> None:
        for data in [self.previous_copyright_data, self.copyright_data]:
            print('calling data.store()')
            data.store()
            print('calling self.archive.get()')
            all_data = self.archive.get()
            print('calling self.list_faculties()')
            faculties = self.list_faculties(all_data)
            print('calling self.create_faculty_sheets()')
            self.create_faculty_sheets(faculties)
            print('calling Sheet().update()')
            all_sheet=Sheet(worksheet_file=self.cip_worksheet_path, sheet_type='all')
            all_sheet.update()
            print('appending all_sheet to self.sheets')
            self.sheets.append(all_sheet)

    def create_faculty_sheets(self, faculties: list[str]) -> None:
        """
        Create a sheet for each faculty in the faculties list.
        """
        for faculty in faculties:
            if faculty is None:
                faculty = "no_faculty_found"
            elif faculty == "":
                faculty = "no_faculty_found"
            sheet_name = str(faculty) + ".xlsx"
            fac_sheet_path = File(os.path.join(self.faculty_sheet_dir.full, sheet_name))


            sheet = Sheet(worksheet_file=fac_sheet_path, sheet_type=faculty)
            sheet.update()
            self.sheets.append(sheet)



    def list_faculties(self, df: pl.DataFrame) -> list[str]:
        """
        return a list of all the faculties in the archive
        """
        faculties = df.select(pl.col("faculty").unique())
        faculties = faculties.to_series().to_list()
        return faculties

if __name__ == "__main__":
    c = duckdb.connect(database='archive.duckdb', read_only=False)
    c.close()

    easy_access = EasyAccess()
    easy_access.run()