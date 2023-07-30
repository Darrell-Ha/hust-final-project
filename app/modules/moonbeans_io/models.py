import peewee
from core.core import get_database
from core.config import DATETIME_FORMATTER

class MoonbeansTradeData(peewee.Model):
    tid = peewee.CharField(max_length=200, primary_key=True)
    contractAddress = peewee.CharField(max_length=50)
    tokenId = peewee.IntegerField()
    buyer = peewee.CharField(max_length=50)
    seller = peewee.CharField(max_length=50, null=True)
    tx = peewee.CharField(max_length=66, null=True)
    value = peewee.FloatField()
    timestamp = peewee.DateTimeField(formats=DATETIME_FORMATTER)

    class Meta:
        database = get_database()
        table_name = "moonbeans_trade_data"



class MoonbeansCollectionData(peewee.Model):

    contractAddress=peewee.CharField(max_length=50, primary_key=True)
    chain=peewee.CharField(max_length=20)
    links=peewee.CharField(max_length=1000, null=True)
    title=peewee.CharField(max_length=100, null=True)
    headerSubtitle=peewee.CharField(max_length=500, null=True)
    fullDescription=peewee.CharField(max_length=5000, null=True)
    startBlock=peewee.IntegerField(null=True)
    owner=peewee.CharField(max_length=50, null=True)
    status=peewee.CharField(max_length=20, null=True)
    enableMetaverse=peewee.BooleanField(null=True)
    enableRarity=peewee.BooleanField(null=True)
    enableBreeding=peewee.BooleanField(null=True)
    enableMint=peewee.BooleanField(null=True)
    enableAttributes=peewee.BooleanField(null=True)
    convertIPFS=peewee.BooleanField(null=True)
    maxSupply=peewee.CharField(max_length=60, null=True)
    totalSupply=peewee.CharField(max_length=60, null=True)
    mintCostText=peewee.CharField(max_length=60, null=True)
    mintBeganText=peewee.CharField(max_length=60, null=True)
    is_archived=peewee.BooleanField()
    created_time=peewee.DateTimeField(formats=DATETIME_FORMATTER)
    updated_time=peewee.DateTimeField(formats=DATETIME_FORMATTER)

    class Meta:
        database = get_database()
        table_name = "moonbeans_collection_data"
        indexes = (
            (("chain", "contractAddress"), True),
        )
