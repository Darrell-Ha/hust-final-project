import peewee
from core.core import get_dw_database


class DimChain(peewee.Model):

    chain_slug=peewee.CharField(max_length=30, primary_key=True)
    relay_chain=peewee.CharField(max_length=20)
    unit=peewee.CharField(max_length=10)
    

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_chain"
        indexes = (
            (("chain_slug",), True),
        )



    