import peewee
from core.core import get_dw_database


class DimSellers(peewee.Model):

    seller_address=peewee.CharField(max_length=60, primary_key=True)
    name=peewee.CharField(max_length=50, null=True)


    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_sellers"
        indexes = (
            (("seller_address",), True),
        )



    