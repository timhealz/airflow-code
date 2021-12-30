from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def get_mysql_db_engine():
    db_creds = BaseHook.get_connection("spendy")

    db_engine = create_engine(
        f"mysql+mysqldb://{db_creds.login}:{db_creds.password}@{db_creds.host}:{db_creds.port}/{db_creds.schema}"
    )

    return db_engine