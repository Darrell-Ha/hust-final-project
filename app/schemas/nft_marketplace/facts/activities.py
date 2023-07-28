import peewee
from core.core import get_dw_database

from ..dims.collections import DimNFT
from ..dims.account import DimAccount
from ..dims.date import DimDate
from ..dims.source_record import DimSource
from ..dims.types_activities import DimTypeActivity



class FactActivities(peewee.Model):

    id_rec=peewee.BigAutoField(primary_key=True)
    date_id=peewee.ForeignKeyField(DimDate, backref="date_activities")
    buyer_address=peewee.ForeignKeyField(DimAccount, backref="ba_activities", null=True)
    seller_address=peewee.ForeignKeyField(DimAccount, backref="sa_activities", null=True)
    nft_id=peewee.ForeignKeyField(DimNFT, backref="ca_activities")
    type_activities=peewee.ForeignKeyField(DimTypeActivity, backref="ta_activities")
    source_record=peewee.ForeignKeyField(DimSource, backref="sr_activities")
    value=peewee.FloatField(null=True)
    unit_usd=peewee.FloatField()
    usd_value=peewee.FloatField()
    tx=peewee.CharField(max_length=80, null=True)

    

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "fact_activities"
        indexes = (
            (("date_id", "buyer_address", "seller_address", "nft_id",\
              "type_activities", "source_record"), True),
        )
