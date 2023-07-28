import peewee
from core.core import get_dw_database


class DimSource(peewee.Model):

    id_source=peewee.AutoField(primary_key=True)
    source_record=peewee.CharField(max_length=30)
    

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_source"
        indexes = (
            (("source_record",), True),
        )

    