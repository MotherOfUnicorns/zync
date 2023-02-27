import sqlite3
import re
from typing import List

import pandas as pd

from .constants import DB_FILE


def query_from_db(qry: str, columns: List[str]) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)

    cursor = con.cursor()
    cursor.execute(qry)
    rows = cursor.fetchall()
    df = pd.DataFrame.from_records(rows, columns=columns)

    con.close()
    return df


def clean_string(my_string: str, replace_space: bool = True):
    my_string = re.sub("[^A-Za-z0-9 ]+", "", my_string).strip()

    if replace_space:
        my_string = my_string.replace(" ", "_")
    return my_string
