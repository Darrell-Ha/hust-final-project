import peewee
from core.core import get_dw_database
from core.config import DATETIME_FORMATTER, DATE_FORMATTER


class DimDate(peewee.Model):

    date_id=peewee.BigIntegerField(primary_key=True)
    date_actual=peewee.DateTimeField(formats=DATETIME_FORMATTER)
    date_time=peewee.DateTimeField(formats=DATE_FORMATTER)
    day_of_week=peewee.IntegerField()
    day_of_month=peewee.IntegerField()
    month=peewee.IntegerField()
    quarter=peewee.IntegerField()
    year=peewee.IntegerField()

    class Meta:
        database = get_dw_database()
        schema = "nft_marketplace"
        table_name = "dim_date"
        indexes = (
            (("date_id",), True),
        )



    