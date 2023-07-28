import peewee
from core.core import get_dw_database
from .chain import DimChain

class DimCollections(peewee.Model):

    contract_with_chain=peewee.CharField(max_length=90, primary_key=True)
    contract_address=peewee.CharField(max_length=50)
    chain_slug=peewee.ForeignKeyField(DimChain, backref="cs_dim_to_clt_dim")
    contract_name=peewee.CharField(max_length=60)
    contract_descriptions=peewee.CharField(max_length=5000, null=True)
    owner_address=peewee.CharField(max_length=100, null=True)
    owner_name=peewee.CharField(max_length=50, null=True)
    link=peewee.CharField(max_length=1000, null=True)
    max_supply=peewee.CharField(max_length=60, null=True)

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_collections"
        indexes = (
            (("contract_address", "chain_slug"), True),
        )


class DimNFT(peewee.Model):

    nft_id=peewee.CharField(max_length=200, primary_key=True)
    contract_with_chain=peewee.ForeignKeyField(DimCollections, backref="clt_dim_to_nft_dim")
    token_id=peewee.CharField(max_length=100)
    token_unique_name=peewee.CharField(max_length=70, null=True)
    token_images=peewee.CharField(max_length=1000, null=True)
    token_descriptions=peewee.CharField(max_length=5000, null=True)

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_nft_token"
        indexes = (
            (("token_id", "contract_with_chain"), True),
        )
