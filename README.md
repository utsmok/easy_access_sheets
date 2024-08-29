# Create basic Easy Access sheets
    by Samuel Mok
    s.mok@utwente.nl
    August 2024

**Quickstart:**

0. Clone this git repo to your local machine & set up python (see notes below)
1. Select the data you want in qlik, export the data as .xlsx
2. Drop the file in the folder ```.\copyright_data```
3. Run the script with ```python .\main.py``` (or ```uv run .\main.py``` if you installed uv, as recommended)
5. Find the processed .xlsx files  in ```.\sheets\<today's date>\```

You can freely use the script on a later date to create additional output. 

**Details**

The script will scan the ```.\exports``` folder and select the most recent file with copyRIGHT data to process, so no need for renaming or selecting a specific file.

It will create a new map in folder ```.\sheets```, using the date of the processed file as the name of the map.
The map will contain one .xlsx file for each faculty, one with all items, and one or two .xlsx files with files where the faculty wasn't or couldn't be determined. 

The script also contains some basic error handling, and will ask the user to confirm overwriting the main data file if one already exists.

If you want to dive into the source code, main.py has a few utility functions and the main function to run the script. functions.py contains all the logic for processing the data and creating the sheets. 

Besides the .xlsx files, the script will also create a .duckdb file in the same folder with all the data, which can be used to produce other data exports or visualizations.

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
$ uv python run main.py
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
