import duckdb
import polars as pl
import dotenv
from rich.console import Console
import datetime
import uuid
import os
import json
print = Console(emoji=True, markup=True).print
dotenv.load_dotenv('settings.env')
class Sheet:
    """
    Manipulate/read/write/copy/compare/update a single worksheet.
    """

    def __init__(self, worksheet_path: str, sheet_type: str):
        """
        Initialize the class with the path to the worksheet file.
        sheet_type should be 'all' or one of the faculty abbreviations (e.g. 'TNW', 'ITC', etc).
        """
        self.path: str = worksheet_path
        try:
            self.current_sheet_data: pl.DataFrame = pl.read_excel(self.path, raise_if_empty=True)
        except FileNotFoundError:
            print(f"File {worksheet_path} does not exist. Creating a new one.")
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
            archive_items = self.archive.get(search_terms=[('faculty',f"'{str(self.sheet_type)}'")])

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
        backup_path = self.path.replace(".xlsx", f"_backup_{today}_{unique_id}.xlsx")
        if not self.current_sheet_data.is_empty():
            self.current_sheet_data.write_excel(backup_path)
            if not os.path.exists(backup_path):
                raise ValueError(f"Backup was not made, stopping script. Please check that the backup file {backup_path} exists.")
        self.new_sheet_data.write_excel(self.path)

    def store_final_data(self) -> None:
        """
        read in the data from the sheet and update the duckdb table 'final_data' with it.
        """
        self.archive.store_final_data(self.current_sheet_data)

class CopyRightData:
    def __init__(self, qlik_export_file: str, dept_mapping_path: str = ''):
        self.qlik_export_file = qlik_export_file
        if dept_mapping_path == '':
            dept_mapping_path = os.path.join(os.path.dirname(__file__), "department_mapping.json")

        self.DEPARTMENT_MAPPING = json.load(open(dept_mapping_path, encoding='utf-8'))
        self.data = self.to_df()
        self.data = self.clean()
        self.data = self.add_faculty_column()
        self.data = self.process()
        self.archive = Archive()


    def to_df(self) -> pl.DataFrame:
        """
        imports data from the Qlik export file into a Polars dataframe
        """
        return pl.read_excel(self.qlik_export_file)

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
        qlik_file_created_date: float = os.path.getctime(self.qlik_export_file)
        # date: from float format to YYYY-MM-DD format
        qlik_file_created_date = datetime.datetime.fromtimestamp(qlik_file_created_date).strftime("%Y-%m-%d")
        return self.data.with_columns(
            pl.Series("retrieved_from_qlik", [qlik_file_created_date] * len(self.data)),
            pl.Series("workflow_status", ["not checked"] * len(self.data)),
            pl.Series("workflow_remarks", ["-"] * len(self.data)),
        )

    def store(self) -> None:
        self.archive.add(self.data)


class Archive:
    def __init__(self, db_path: str|None = None):
        if not db_path:
            self.db_path = os.getenv("DUCKDB_PATH")
            if not self.db_path:
                raise ValueError("No database path provided and no DUCKDB_PATH environment variable found.")
        else:
            self.db_path = db_path
        #self.print_archive_columns()


    def print_archive_columns(self) -> None:
        with duckdb.connect(database=self.db_path, read_only=True) as con:
            print(con.execute("SELECT * FROM archive").pl().glimpse())

    def add(self, df: pl.DataFrame):
        '''
        takes in a dataframe from the DataImporter, and adds any rows not currently in the archive. If no archive exists, create it.
        Make sure to check all the fields with dates like 'last_modified' and 'retrieved_from_qlik' are correct.
        If rows already exist, ignore them (keep the old ones).
        '''

        # first check the df for errors
        df = self.check_dataframe(df)
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            # if archive doesn't exist, create it
            if not con.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='archive';""").fetchall():
                con.execute(f"""
                    CREATE TABLE 'archive' AS SELECT * FROM df;
                    """)
            else:
                # if archive exists, add only rows which are not already in the archive
                # check this by comparing the material_id column
                existing_material_ids = con.execute("SELECT material_id FROM archive").pl()
                new_material_ids = df.filter(~df['material_id'].is_in(existing_material_ids['material_id']))
                if not new_material_ids.is_empty():
                    con.execute("INSERT INTO archive SELECT * FROM new_material_ids")
                else:
                    print(f"No new rows to add to the archive.")

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

        with duckdb.connect(database=self.db_path, read_only=True) as con:
            if search_terms is None:
                return con.execute("SELECT * FROM archive").pl()
            else:
                #TODO: test this!!
                return con.execute(f"SELECT * FROM archive WHERE {' AND '.join([f'{key} = {value}' for key, value in search_terms])}").pl()

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

        self.copyright_data_dir = os.getenv("QLIK_EXPORTS_DIR")
        self.faculty_sheet_dir = os.getenv("FACULTY_SHEETS_DIR")
        self.cip_worksheet_path = os.path.join(os.getenv("CIP_WORKSHEET_DIR"), "all.xlsx")
        self.copyright_data, self.previous_copyright_data = self.get_latest_export()
        self.sheets: list[Sheet] = []

    def get_latest_export(self) -> tuple[CopyRightData, CopyRightData]:
        """
        Scan all files in self.copyright_data_dir and return the 2 latest ones.
        Use the created date to determine which file to return.
        """
        all_files = os.listdir(self.copyright_data_dir)
        latest_file = max([os.path.join(self.copyright_data_dir, file) for file in all_files], key=os.path.getctime)
        previous_file = max([os.path.join(self.copyright_data_dir, file) for file in all_files if os.path.join(self.copyright_data_dir, file) != latest_file], key=os.path.getctime)
        return CopyRightData(qlik_export_file=latest_file), CopyRightData(qlik_export_file=previous_file)

    def run(self) -> None:
        for data in [self.previous_copyright_data, self.copyright_data]:
            data.store()
            all_data = self.archive.get()
            faculties = self.list_faculties(all_data)

            self.create_faculty_sheets(faculties)
            all_sheet=Sheet(worksheet_path=self.cip_worksheet_path, sheet_type='all')
            all_sheet.update()
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
            fac_sheet_path = os.path.join(os.getcwd(), self.faculty_sheet_dir, sheet_name)


            sheet = Sheet(worksheet_path=fac_sheet_path, sheet_type=faculty)
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
    easy_access = EasyAccess()
    easy_access.run()