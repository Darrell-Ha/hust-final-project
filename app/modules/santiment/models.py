import peewee
from core.core import get_database

class SantimentPriceChain(peewee.Model):
    
    id=peewee.AutoField()
    time=peewee.DateTimeField(formats="%Y-%m-%dT%H:%M:%S.%fZ")
    marketcap_usd=peewee.FloatField(null=True)
    price_usd=peewee.FloatField(null=True)
    volume_usd=peewee.FloatField(null=True)
    chain=peewee.CharField(max_length=20, null=True)


    class Meta:
        database = get_database()
        table_name = "santiment_price_chain"
        indexes = (
            (("chain","time"), True),
        )