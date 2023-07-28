import peewee
from core.core import get_dw_database


class DimTypeActivity(peewee.Model):

    id_type=peewee.AutoField(primary_key=True)
    type_activities=peewee.CharField(max_length=20)
    

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_type_act"
        indexes = (
            (("type_activities",), True),
        )

    