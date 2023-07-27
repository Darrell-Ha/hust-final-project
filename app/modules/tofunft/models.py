import peewee
from core.core import get_database

class TofuTradeData(peewee.Model):
    
    id_rec=peewee.IntegerField(primary_key=True, null=False)
    tx=peewee.CharField(max_length=80, null=True)
    category=peewee.CharField(max_length=15, null=False)
    type_tx=peewee.CharField(max_length=30, null=True)
    price=peewee.FloatField(null=True)
    to_user_nickname=peewee.CharField(max_length=30, null=True)
    to_address=peewee.CharField(max_length=50, null=True)
    from_user_nickname=peewee.CharField(max_length=30, null=True)
    from_address=peewee.CharField(max_length=50, null=False)
    nft_name=peewee.CharField(max_length=60, null=True)
    nft_description=peewee.CharField(max_length=2000, null=True)
    token_id=peewee.CharField(max_length=100, null=False)
    nft_contract=peewee.CharField(max_length=50, null=False)
    contract_name=peewee.CharField(max_length=50, null=False)
    owner=peewee.CharField(max_length=50, null=False)
    created_at=peewee.DateTimeField(formats="%Y-%m-%d %H:%M:%S")
    network=peewee.CharField(max_length=15)

    class Meta:
        database = get_database()
        table_name = "tofunft_trade_data"

