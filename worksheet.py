import marimo

__generated_with = "0.8.11"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import polars as pl
    import duckdb
    import ibis
    import datetime
    import uuid
    import os
    import json
    import pathlib
    import shutil
    import dotenv

    import new
    return (
        datetime,
        dotenv,
        duckdb,
        ibis,
        json,
        mo,
        new,
        os,
        pathlib,
        pl,
        shutil,
        uuid,
    )


@app.cell
def __(new):
    easy_access = new.EasyAccess()

    return easy_access,


@app.cell
def __(easy_access):
    easy_access.run()
    return


@app.cell
def __(easy_access):
    archive_table = easy_access.archive.get()
    return archive_table,


@app.cell
def __(archive_table, mo):
    mo.ui.dataframe(archive_table)
    return


if __name__ == "__main__":
    app.run()
