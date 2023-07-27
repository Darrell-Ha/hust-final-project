import peewee
from core.core import get_database
from core.config import DATETIME_FORMATTER
class NftradeData(peewee.Model):
    
    idTrade=peewee.CharField(max_length=50, primary_key=True)
    chainId=peewee.IntegerField()
    typeRec=peewee.CharField(max_length=30, null=True)
    fromUser=peewee.CharField(max_length=50, null=True)
    toUser=peewee.CharField(max_length=50, null=True)
    createdAt=peewee.DateTimeField(formats=DATETIME_FORMATTER)
    price=peewee.FloatField()
    contractAddress=peewee.CharField(max_length=50)
    contractName=peewee.CharField(max_length=50)
    tokenId=peewee.CharField(max_length=100)
    tokenName=peewee.CharField(max_length=50)
    tokenImage=peewee.CharField(max_length=1000, null=True)
    side=peewee.CharField(max_length=20, null=True)

    class Meta:
        database = get_database()
        table_name = "nft_trade_data"

