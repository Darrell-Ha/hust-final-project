import peewee
from core.core import get_dw_database


class DimAccount(peewee.Model):

    address=peewee.CharField(max_length=60, primary_key=True)
    name=peewee.CharField(max_length=50, null=True)


    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_account"
        indexes = (
            (("address", ), True),    
        )
        
    