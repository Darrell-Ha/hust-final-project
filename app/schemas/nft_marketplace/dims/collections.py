import peewee
from core.core import get_dw_database


class DimCollections(peewee.Model):

    contract_address=peewee.CharField(max_length=50, primary_key=True)
    contract_name=peewee.CharField(max_length=60)
    descriptions=peewee.CharField(max_length=5000, null=True)
    owner_address=peewee.CharField(max_length=100, null=True)
    owner_name=peewee.CharField(max_length=50, null=True)
    link=peewee.CharField(max_length=1000, null=True)
    max_supply=peewee.CharField(max_length=60, null=True)
    total_supply=peewee.CharField(max_length=60, null=True)

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_collections"
        indexes = (
            (("contract_address",), True),
        )

    