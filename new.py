import duckdb
import polars as pl
import dotenv
from rich.console import Console
import datetime
import uuid
import os
import json
import ibis

from file_utils import Directory, File

print = Console(emoji=True, markup=True).print
dotenv.load_dotenv('settings.env')

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
        self._data = self.to_df()
        self._data = self.clean()
        self._data = self.add_faculty_column()
        self._data = self.process()

    @property
    def data(self) -> pl.DataFrame:
        return self._data
    
    def to_df(self) -> pl.DataFrame:
        """
        imports data from the Qlik export file into a Polars dataframe
        """
        if not self._data:
            return pl.read_excel(self.qlik_export_file.path)
        else:
            return self._data

    def clean(self) -> pl.DataFrame:
        """
        cleans and preprocesses the imported data
        """
        return self._data.rename(
            lambda col: col.replace(" ", "_")
            .replace("#", "count_")
            .replace("*", "x")
            .lower()
            )

    def add_faculty_column(self) -> pl.DataFrame:
        return self._data.with_columns(
            faculty=pl.col("department").replace_strict(
                self.DEPARTMENT_MAPPING, default="Unmapped"
            )
        )
    
    def process(self) -> pl.DataFrame:
        """
        adds the extra columns (e.g. faculty, workflow_status, workflow_remarks, ...), checks for errors, adds 'retrieved_from_qlik' date, etc
        """
        qlik_file_created_date: str = self.qlik_export_file.created().strftime("%Y-%m-%d")
        return self._data.with_columns(
            pl.Series("retrieved_from_qlik", [qlik_file_created_date] * len(self._data)),
            pl.Series("workflow_status", ["not checked"] * len(self._data)),
            pl.Series("workflow_remarks", ["-"] * len(self._data)),
        )

    

class Archive:
    '''
    This class handles  duckdb interactions.
    It stores the archive of  the data.
    Currently this is just a list of the qlik data as it was initially imported.
    
    '''
    def __init__(self, db_path: str|File|None = None):
        self.item_history_key_columns = ['classification', 'ml_prediction', 'manual_classification', 'last_change', 'status']

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
        
        if 'item_history' in self.con.list_tables():
            self.item_history = self.con.table('item_history')
        else:
            self.item_history = None
        
        if 'current' in self.con.list_tables():
            self.current = self.con.table('current')
        else:
            self.current = None

    def update(self, data: CopyRightData):
        '''
        Ingest a new CopyRightData export file into the archive.
        Will update:
            'current' table (all rows as currently shown in qlik),
            'item_history' table (a log of all rows as they were exported from qlik, so we can keep track of changes),
            'archive' table (1 row per material_id with the status as they were first imported).
        
        '''
        df = self.check_dataframe(data._data)
        self.update_current(df)
        self.update_item_history(df)
        self.update_archive()

    def update_current(self, df: pl.DataFrame):
        '''
        takes in a dataframe with CopyRightData, and does the following:
        - for each existing material_id, update key columns (classification, ml_prediction, manual_classification, last_change, status) with the new values
        - for each new material_id, insert a new row into the  table
        
        '''

        # first check the df for errors
        with duckdb.connect(database='archive.duckdb', read_only=False) as con:
            if self.current is None:
                con.execute("""
                    CREATE TABLE 'current' AS SELECT * FROM df;
                    """)
                self.current = self.con.table('current')
            else:
                con.execute("CREATE OR REPLACE TEMP TABLE new_data AS SELECT * FROM df")
                con.execute("""
                    MERGE INTO current AS target
                    USING new_data AS source
                    ON target.material_id = source.material_id
                    WHEN MATCHED AND (
                        target.classification != source.classification OR
                        target.ml_prediction != source.ml_prediction OR
                        target.manual_classification != source.manual_classification OR
                        target.last_change != source.last_change OR
                        target.status != source.status
                    ) THEN
                        UPDATE SET
                            classification = source.classification,
                            ml_prediction = source.ml_prediction,
                            manual_classification = source.manual_classification,
                            last_change = source.last_change,
                            status = source.status
                    WHEN NOT MATCHED THEN
                        INSERT VALUES (source.*)
                """)
                self.current = self.con.table('current')

    def update_item_history(self, df: pl.DataFrame):
        with duckdb.connect(database=self.db_path, read_only=False) as con:
            if self.item_history is None:
                con.execute("""
                    CREATE TABLE 'item_history' AS SELECT * FROM df;
                    """)
                self.item_history = self.con.table('current')
            else:
                con.execute("CREATE OR REPLACE TEMP TABLE new_data AS SELECT * FROM df")
                con.execute("""
                    INSERT INTO item_history
                    SELECT * 
                    FROM new_data
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM item_history
                        WHERE item_history.material_id = new_data.material_id
                        AND item_history.classification = new_data.classification
                        AND item_history.ml_prediction = new_data.ml_prediction
                        AND item_history.manual_classification = new_data.manual_classification
                        AND item_history.last_change = new_data.last_change
                        AND item_history.status = new_data.status
                    )
                """)
                self.item_history = self.con.table('item_history')


    def update_archive(self, df: pl.DataFrame | None = None):
        '''
        If no archive exists, create it using the df.
        If an archive exists, add only rows which are not already in the archive.
        If no df is provided, use the item_history table to update the archive.
        
        If rows already exist, ignore them (keep the old ones).
        '''
        with duckdb.connect(database='archive.duckdb', read_only=False) as con:
            if df is not None:
                if self.archive is None:
                    con.execute("""
                        CREATE TABLE 'archive' AS SELECT * FROM df;
                        """)
                    self.archive = self.con.table('archive')
                else:
                    con.execute("CREATE OR REPLACE TEMP TABLE new_data AS SELECT * FROM df")
                    con.execute("""
                        INSERT INTO archive
                        SELECT new_data.* 
                        FROM new_data
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM archive
                            WHERE archive.material_id = new_data.material_id
                        )
                    """)

                    self.archive = self.con.table('archive')
            else:
                con.execute("""
                    INSERT INTO archive
                    SELECT 
                        ih.*
                    FROM item_history ih
                    INNER JOIN (
                        SELECT material_id, MIN(retrieved_from_qlik) as min_retrieved_date
                        FROM item_history
                        WHERE material_id NOT IN (SELECT material_id FROM archive)
                        GROUP BY material_id
                    ) AS earliest_entries
                    ON ih.material_id = earliest_entries.material_id 
                    AND ih.retrieved_from_qlik = earliest_entries.min_retrieved_date
                    WHERE NOT EXISTS (
                        SELECT 1 FROM archive a
                        WHERE a.material_id = ih.material_id
                    )
                """)
                self.archive = self.con.table('archive')
                
    def store_final_data(self, df: pl.DataFrame) -> None:
        '''
        Store the data from the worksheet in the final_data table in the duckdb archive.
        '''


    def get(self, data: str = 'archive', search_terms: list[tuple[str, str]]|None = None) -> pl.DataFrame:
        '''
        get specific rows from an archive table
        data should be one of 'archive', 'current', or 'item_history' -- i.e. the name of the table to get data from
        search_terms should be a list with dictionaries with shape {'field_name': 'value'}
        if search_terms is None, return all rows
        '''
        if data == 'archive':
            table = self.archive
        elif data == 'current':
            table = self.current
        elif data == 'item_history':
            table = self.item_history

        if search_terms is None:
            return table.to_polars()
        else:
            #TODO: test this!!
            # return all rows from self.archive where all conditions in the search terms are met
            # search terms are a list of tuples, where the first element is the column name, and the second element is the value to match
            # e.g. [('material_id', '12345'), ('workflow_status', 'not checked')]
            filtered = table
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
        all_files = self.copyright_data_dir.files
        latest_file = max(all_files, key=lambda x: x.created())
        previous_file = max(all_files, key=lambda x: x.created(), default=latest_file)
        return CopyRightData(qlik_export_file=latest_file), CopyRightData(qlik_export_file=previous_file)

    def run(self) -> None:
        for data in [self.previous_copyright_data, self.copyright_data]:
            print('storing data in archive')
            self.archive.update(data)
            print('calling self.archive.get()')
            all_data = self.archive.get(data='archive', search_terms=None)
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