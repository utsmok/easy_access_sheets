# Create basic Easy Access sheets
    by Samuel Mok
    s.mok@utwente.nl
    August 2024


# NOTE!

This readme is a work in progress. The current script already works differently -- we moved to living worksheets.
Documentation will be updated in the future.

# Basic usage

To set up the script, you'll first need to download the contents of this repository to your local machine. git is recommended of course.
If you're unfamiliar with git, I'd suggest using the github interface to download the repo as a zip file. Visit the [repo](https://github.com/utsmok/easy_access_sheets) on GitHub and press the big green button 'Code' (next to 'About'), and select 'Download ZIP'. Extract the zip file to a folder on your computer, and then open a command prompt / PowerShell / terminal in that folder.

Then, you'll need to set up python & install the required depencies to run the script. See **Install / Usage** below for recommended python installation instructions.


## Creating the main sheet

1. Select the data you want in qlik, export the data as .xlsx
2. Drop the file in the repository folder ```.\copyright_data```
3. Navigate to the repository in your terminal/command prompt and run the script with ```python .\main.py``` (or ```uv run .\main.py``` if you installed uv, as recommended)
4. Find the processed .xlsx file in ```.\cipworksheets\<today's date>\```

You can freely use the script on a later date to create additional output -- but make sure to delete or move the created .xlsx files before doing so, or else the script will throw an error to prevent accidental overwriting.

## Creating faculty sheets

The sheets for individual faculties can be created by first creating the main sheet, and then running the script with the commandline option 'faculty'.
If you try to create the faculty sheets without first creating the main sheet, the script will generate the main sheet for you -- but this is not recommended, as it'll be difficult to check the output for errors before sharing it.

1. Run the script with the commandline option 'faculty', like so: ```python .\main.py --faculty``` (or ```uv run .\main.py --faculty``` if you installed uv, as recommended)
2. The created sheets per faculty will be stored in ```.\sheets\<today's date>\```

## Changing parameters

The file 'department_mapping.json' contains the mapping between the 'department' column in the copyRIGHT data and the corresponding faculty. Change this file if there are errors in the mapping.

# Some background info

The script will scan the ```.\copyright_data``` folder and select the most recent file with copyRIGHT data to process, so no need for renaming or selecting a specific file.

It will create a new map in folder ```.\cipworksheets```, using the date of the processed file as the name of the map.
The map will contain one .xlsx file with all items, including a column 'faculty' denoting the faculty of the programme that the item belongs to.
Besides the .xlsx file, the script will also create a .duckdb file in the same folder with all the data, which can be used to produce other data exports or visualizations.

If run with the ```--faculty``` option, the script will create a new folder inside ```.\sheets``` with the date of the last export and containing one .xlsx file per faculty,  one with all items, and one or two .xlsx files with files where the faculty wasn't or couldn't be determined.

The script also contains some basic error handling, and will ask the user to confirm overwriting main data files if they already exist.
If you want to dive into the source code, main.py holds the functions for file & path handling and the main function to run the script. functions.py contains all the logic for processing the data and creating the sheets.


## Install / Usage

I suggest using [uv](https://github.com/astral-sh/uv) to manage your python install, dependencies, and virtual environments. It's quick, easy, and works on all platforms. It's great!

Installation is easy. Pick the one that fits your platform:

```bash
# On macOS and Linux (Bash).
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows (PowerShell).
$ powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# With pip (generic).
$ pip install uv
```

Then, once installed, execute the following commands to install a fresh python version using uv, and let it do the rest for you:

```bash
$ uv python install
$ uv run main.py
```

Easy, right? If you want to do the steps yourself using uv, you can do so as well:

```bash
# grab the latest python and make a virtual environment
$ uv python install
$ uv venv

# now activate the virtual environment -- see the output of the previous command
$ source venv/bin/activate

# install dependencies
$ uv pip install -r pyproject.toml

# run the project
$ uv run main.py
```

### Dependencies
- duckdb
- fastexcel
- polars
- pyarrow
- rich
- xlsxwriter
