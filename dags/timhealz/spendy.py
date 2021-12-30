from datetime import datetime
import json
import logging
import mintapi
import os

from decimal import Decimal
from re import sub
from typing import Dict

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.macros import ds_format
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator

from sqlalchemy.orm import sessionmaker

from timhealz.common.utils import get_mysql_db_engine
from timhealz.common.data_models.spendy import Transaction

log = logging.getLogger(__name__)


DATA_DIR = Variable.get("MINT_DATA_DIRECTORY")
TRANSACTIONS_FP = os.path.join(DATA_DIR, "transactions/{ds}.json")


def parse_transaction(transaction: Dict, ds: str) -> Transaction:
    return Transaction(
        date = transaction.get("date"),
        note = transaction.get("note"),
        is_percent = transaction.get("isPercent"),
        financial_institution = transaction.get("fi"),
        transaction_type = transaction.get("txnType"),
        number_matched_by_rule = transaction.get("numberMatchedByRule"),
        is_edited = transaction.get("isEdited"),
        is_pending = transaction.get("isPending "),
        mcategory = transaction.get("mcategory"),
        is_matched = transaction.get("isMatched"),
        odate = transaction.get("odate"),
        is_first_date = transaction.get("isFirstDate"),
        id = transaction.get("id"),
        is_duplicate = transaction.get("isDuplicate"),
        has_attachments = transaction.get("hasAttachments"),
        is_child = transaction.get("isChild"),
        is_spending = transaction.get("isSpending"),
        amount = Decimal(sub(r'[^\d.]', '', transaction.get("amount"))),
        rule_category = transaction.get("ruleCategory"),
        user_category_id = transaction.get("userCategoryId"),
        is_transfer = transaction.get("isTransfer"),
        is_after_creation_time= transaction.get("isAfterFiCreationTime"),
        merchant = transaction.get("merchant"),
        manual_type = transaction.get("manualType"),
        labels = str(transaction.get("labels")),
        mmerchant = transaction.get("mmerchant"),
        is_check = transaction.get("isCheck"),
        omerchant = transaction.get("omerchant"),
        is_debit = transaction.get("isDebit"),
        category = transaction.get("category"),
        rule_merchant = transaction.get("ruleMerchant"),
        is_linked_to_rule = transaction.get("isLinkedToRule"),
        account = transaction.get("account"),
        category_id = transaction.get("categoryId"),
        rule_category_id = transaction.get("ruleCategoryId"),
        ds = ds
    )


with DAG(
    dag_id='Spendy',
    schedule_interval="0 14 * * *",
    start_date=datetime(2021, 12, 28),
    catchup=True,
    tags=['personal-finance'],
) as dag:

    @task(task_id="get_mint_transactions")
    def get_mint_transactions(ds=None, **kwargs):

        log.info("Fetching intuit credentials")
        intuit_creds = BaseHook.get_connection("intuit-credentials")

        log.info("Initializing Mint session")
        mint = mintapi.Mint(
            intuit_creds.login,
            intuit_creds.password,
            headless=True,
            use_chromedriver_on_path=True,
        )

        mint_ds = ds_format(ds, "%Y-%m-%d", "%m/%d/%y")

        log.info("Getting transactions")
        data = mint.get_transactions_json(
            start_date=mint_ds, end_date=mint_ds
        )

        fp = TRANSACTIONS_FP.format(ds=ds)
        log.info(f"Dumping transactions json to {fp}")
        with open(fp, "w") as out:
            json.dump(data, out, indent=4)

    get_mint_transactions = get_mint_transactions()


    db_clear_mint_transactions = MySqlOperator(
        task_id="db_clear_mint_transactions",
        mysql_conn_id="spendy",
        sql="""
        DELETE FROM spendy.mint_transactions
        WHERE
            ds = '{{ ds }}';
        """,
    )

    get_mint_transactions >> db_clear_mint_transactions


    @task(task_id="db_insert_mint_transactions")
    def insert_mint_transactions(ds=None, **kwargs):

        fp = TRANSACTIONS_FP.format(ds=ds)
        with open(fp, "r") as f:
            transactions = json.load(f)

        Session = sessionmaker(bind=get_mysql_db_engine())
        session = Session()

        log.info(f"Loading transactions")
        for transaction in transactions:
            log.info(transaction)
            session.add(parse_transaction(transaction=transaction, ds=ds))

        session.commit()
    
    insert_mint_transactions = insert_mint_transactions()


    db_clear_mint_transactions >> insert_mint_transactions
