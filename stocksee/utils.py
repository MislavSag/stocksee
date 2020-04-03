import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import janitor


def requests_retry_session(retries=5, backoff_factor=0.3, 
                           status_forcelist=(500, 502, 504), session=None):
    """
    General session function that handles errors and make retries. Return session.
    Source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    })
    return session


def get_request(url_req, ver=True):
    """
    Get request.
    Source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    try:
        response = requests_retry_session().get(url_req, verify=ver, timeout=60)
        response.encoding = "utf-8"
        print('It eventually worked', response.status_code)
        return response
    except Exception as x:
        print('It failed :(', x.__class__.__name__)       
     
  
def write_to_db(df, database_name, table_name, primary_key=True):
    """
    Creates a sqlalchemy engine and write the dataframe to database
    Source: https://stackoverflow.com/questions/55750229/how-to-save-a-data-frame-as-a-table-in-sql
    """
    # replacing infinity by nan
    df = df.replace([np.inf, -np.inf], np.nan)

    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@91.234.46.219/{db}"
                           .format(user="odvjet12_mislav",
                                   pw="Theanswer0207",
                                   db=database_name))

    # Write to DB
    df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=100)
    with engine.connect() as con:
        con.execute('ALTER table ' + table_name + ' add id int primary key auto_increment;')


def first_column_to_names(tbl):
    """
    First column of pd table to column names and clean names. Return pd table
    """
    tbl = tbl.dropna(how='all')
    tbl.columns = list(range(len(tbl.columns)))
    tbl = tbl.pivot_table(values=1, columns=0, aggfunc='first')
    return janitor.clean_names(tbl)


def rbind(tables):
    """
    Bind tables by rows. Remove None tables if exists.
    """
    tbl_removed_none = [tbl for tbl in tables if tbl is not None]
    tbl_reset_index = [tbl.reset_index(drop=True) for tbl in tbl_removed_none]
    return pd.concat(tbl_reset_index, axis=1, sort=False)


def str_to_float(currencynum, thousand=r'\.', decimal=",", regex_pattern=r"\d+[,]\d+"):
    """
    Convert string that contain number to numeric.
    """
    if isinstance(currencynum, str):
        num = re.sub(thousand, "", currencynum)
        num = re.findall(regex_pattern, num)
        if num:
            num = re.sub(decimal, ".", num[0])
            return float(num)
        else:
            return np.nan
    else:
        return np.nan


def query_to_db(query, database_name):
    """
    Get dat from database.
    """
    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@91.234.46.219/{db}"
                           .format(user="odvjet12_mislav",
                                   pw="Theanswer0207",
                                   db=database_name))

    # Write to DB
    return pd.read_sql(query, engine)


def delete_rows(sql_delete_query, database_name):
    """
    Delete rows from db.
    """
    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@91.234.46.219/{db}"
                           .format(user="odvjet12_mislav",
                                   pw="Theanswer0207",
                                   db=database_name))

    # Write to DB
    engine.execute(sql_delete_query)
    
    # close connection
    engine.close()
    print("MySQL connection is closed")


def write_to_db_update(df, database_name, table_name):
    """
    Creates a sqlalchemy engine and write the dataframe to database
    Source: https://stackoverflow.com/questions/55750229/how-to-save-a-data-frame-as-a-table-in-sql
    """
    # replacing infinity by nan
    df = df.replace([np.inf, -np.inf], np.nan)

    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@91.234.46.219/{db}"
                           .format(user="odvjet12_mislav",
                                   pw="Theanswer0207",
                                   db=database_name))

    # Write to DB
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=100)
