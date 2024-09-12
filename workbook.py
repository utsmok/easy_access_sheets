import marimo

__generated_with = "0.8.11"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import os
    import polars as pl
    import duckdb
    import datetime
    import json
    import uuid
    import dotenv
    import new

    return datetime, dotenv, duckdb, json, mo, new, os, pl, uuid


@app.cell
def __(new):
    new.EasyAccess()
    return


if __name__ == "__main__":
    app.run()
