import logging
# import peewee_async
import peewee
import core.config as config

_db = peewee.PostgresqlDatabase(config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD,
                                      host=config.DB_HOST, port=config.DB_PORT)
_dw_db = peewee.PostgresqlDatabase(config.DB_DW_NAME, user=config.DB_USER, password=config.DB_PASSWORD,
                                      host=config.DB_HOST, port=config.DB_PORT)

# _db.set_allow_sync(config.ALLOW_SYNC)
# _dw_db.set_allow_sync(config.ALLOW_SYNC)
# _objects = peewee.Manager(_db)
# _dw_objects = peewee.Manager(_dw_db)

_logger = logging.getLogger(config.LOG_NAME)
_logger.setLevel(getattr(logging, config.LOG_LEVEL))
_sHandle = logging.StreamHandler()
_sHandle.setLevel(getattr(logging, config.LOG_LEVEL))
_sHandle.setFormatter(logging.Formatter(config.LOG_FORMATTER))
_logger.addHandler(_sHandle)
if config.LOG_FILE:
    fHandle = logging.FileHandler(config.LOG_FILE)
    fHandle.setLevel(getattr(logging, config.LOG_LEVEL))
    fHandle.setFormatter(logging.Formatter(config.LOG_FORMATTER))
    _logger.addHandler(fHandle)




## RAW_DB
def get_database():
    return _db


# def get_database_manager():
#     return _objects


## DW_DB
def get_dw_database():
    return _dw_db


# def get_dw_database_manager():
#     return _dw_objects


def get_logger():
    return _logger
