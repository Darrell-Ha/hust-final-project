import peewee
from core.core import get_dw_database

from ..dims.collections import DimCollections
from ..dims.buyers import DimBuyers
from ..dims.sellers import DimSellers
from ..dims.date import DimDate
from ..dims.chain import DimChain



class FactActivities(peewee.Model):

    # id_rec=peewee.BigIntegerField(primary_key=True)
    date_id=peewee.ForeignKeyField(DimDate, backref="date_activities")
    contract_address=peewee.ForeignKeyField(DimCollections, backref="ca_activities")
    buyer_address=peewee.ForeignKeyField(DimBuyers, backref="ba_activities", null=True)
    seller_address=peewee.ForeignKeyField(DimSellers, backref="sa_activities", null=True)
    chain_slug=peewee.ForeignKeyField(DimChain, backref="cs_activities", null=True)
    type_activities=peewee.CharField(max_length=20)
    token_id=peewee.CharField(max_length=100, null=True)
    value=peewee.FloatField(null=True)
    # unit=peewee.CharField(max_length=10, null=True)
    unit_usd=peewee.FloatField(null=True)
    usd_value=peewee.FloatField(null=True)
    source_record=peewee.CharField(max_length=30, null=True)
    tx=peewee.CharField(max_length=80, null=True)

    

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "fact_activities"
        indexes = (
            (("dim_id", "contract_address",), True),
        )
