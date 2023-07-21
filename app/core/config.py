import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_DW_NAME = os.getenv('DB_DW_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# # applied for peewee_async
# ALLOW_SYNC = True

SUBSCAN_API_KEY = os.getenv("SUBSCAN_API_KEY")
LOG_NAME = os.getenv("LOG_NAME")


LOG_FILE = f"{LOG_NAME}.log"
LOG_LEVEL = "DEBUG"
LOG_FORMATTER = "[%(asctime)s] [%(name)s] [%(levelname)s] : %(message)s"

