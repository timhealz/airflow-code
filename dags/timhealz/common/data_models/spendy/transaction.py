from sqlalchemy import (
    Column,
    #ForeignKey,
    Numeric,
    BigInteger,
    String,
    Date,
    DateTime,
    Boolean,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

from timhealz.common.utils import get_mysql_db_engine


Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'mint_transactions'

    date =  Column(String(255))
    note =  Column(String(255))
    is_percent =  Column(Boolean)
    financial_institution =  Column(String(255))
    transaction_type = Column(BigInteger)
    number_matched_by_rule = Column(BigInteger)
    is_edited =  Column(Boolean)
    is_pending =  Column(Boolean)
    mcategory =  Column(String(255))
    is_matched =  Column(Boolean)
    odate =  Column(String(255))
    is_first_date =  Column(Boolean)
    id = Column(BigInteger, primary_key=True)
    is_duplicate =  Column(Boolean)
    has_attachments =  Column(Boolean)
    is_child =  Column(Boolean)
    is_spending =  Column(Boolean)
    amount = Column(Numeric(10, 2))
    rule_category =  Column(String(255))
    user_category_id =  Column(String(255))
    is_transfer =  Column(Boolean)
    is_after_creation_time =  Column(Boolean)
    merchant =  Column(String(255))
    manual_type = Column(BigInteger)
    labels = Column(String(255))
    mmerchant = Column(String(255))
    is_check = Column(Boolean)
    omerchant = Column(String(255))
    is_debit = Column(Boolean)
    category = Column(String(255))
    rule_merchant = Column(String(255))
    is_linked_to_rule = Column(Boolean)
    account = Column(String(255))
    category_id = Column(BigInteger)
    rule_category_id = Column(BigInteger)
    ds = Column(Date)
    process_ts = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    def __repr__(self):
        return f"<Transaction(ds='{self.ds}', description='{self.note}', amount={self.amount})>"

    def get_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


if __name__ == "__main__":
    db_engine = get_mysql_db_engine()

    Base.metadata.create_all(db_engine)
