import airflow.macros as macros

from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def get_mysql_db_engine():
    db_creds = BaseHook.get_connection("spendy")

    db_engine = create_engine(
        f"mysql+mysqldb://{db_creds.login}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.schema}"
    )

    return db_engine


def mint_date_to_ds(mint_date: str) -> str:
    try:
        month, day = mint_date.split()
        ds = macros.ds_format(f"2021-{month}-{day}","%Y-%b-%d", "%Y-%m-%d")
    except:
        ds = macros.ds_format(mint_date, "%m/%d/%y", "%Y-%m-%d")
    
    return ds