from .dims.account import DimAccount
from .dims.date import DimDate
from .dims.collections import DimCollections
from .dims.chain import DimChain
from .facts.activities import FactActivities

from modules.santiment.models import SantimentPriceChain
from core.base_transform_load import BaseTransformLoad
from core.core import get_logger
from core.config import DATETIME_FORMATTER
import pandas as pd
import json
import datetime
import traceback
import peewee



class NftTransformer(BaseTransformLoad):
    
    # START_TIME = datetime.datetime.now()-datetime.timedelta(days=2)
    START_TIME = datetime.datetime(2022, 1, 18)
    # END_TIME = datetime.datetime.now()
    END_TIME = datetime.datetime.now()
    FORMAT_TIME_MAPPING = "%Y-%m-%d %H"

    def get_data_price(self, chains: list):
        df = pd.DataFrame(
            SantimentPriceChain\
                .select(
                    SantimentPriceChain.time,
                    SantimentPriceChain.price_usd,
                    SantimentPriceChain.chain
                )\
                .where(
                    SantimentPriceChain.time.between(self.START_TIME, self.END_TIME)\
                    & SantimentPriceChain.chain.in_(chains)  
                )\
                .dicts()
        )
        df['time_price_mapping'] = df['time'].dt.strftime(self.FORMAT_TIME_MAPPING)
        df = df.rename(columns={"price_usd": "unit_usd"})
        return df

    def init_table(self):
        try:
            if not DimAccount.table_exists():
                DimAccount.create_table(safe=True)
            if not DimDate.table_exists():
                DimDate.create_table(safe=True)
            if not DimChain.table_exists():
                DimChain.create_table(safe=True)
            if not DimCollections.table_exists():
                DimCollections.create_table(safe=True)
            if not FactActivities.table_exists():
                FactActivities.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def transform(self) -> pd.DataFrame:
        pass

    def bulk_load(self, records: pd.DataFrame):
        records = records.sort_values('date_id', ascending=False)
        self.load_to_date(records[['date_id']])
        self.load_to_account(records[['seller_address', 'seller_name']])
        self.load_to_account(records[['buyer_address', 'buyer_name']])
        self.load_to_chain(records[['chain_slug']])
        self.load_to_collection(records[['contract_address', 'contract_name', 'descriptions',\
                                         'owner_address',  'owner_name', 'link', 'max_supply', 'total_supply']])
        self.load_to_activities(records[["date_id", "contract_address", "buyer_address", "seller_address",\
                                        "chain_slug", "type_activities", "token_id", "value", "unit_usd",\
                                        "usd_value", "source_record", "tx"]])
        pass

    def load_to_date(self, date_ids: pd.DataFrame):
        process_time = date_ids.drop_duplicates().copy()
        process_time['date_actual'] = pd.to_datetime(process_time['date_id']).dt.strftime(DATETIME_FORMATTER)
        process_time['day_of_week'] = process_time['date_id'].dt.day_of_week + 2
        process_time['day_of_month'] = process_time['date_id'].dt.day
        process_time['month'] = process_time['date_id'].dt.month
        process_time['quarter'] = (process_time['month'] - 1) // 3 + 1 
        process_time['year'] = process_time['date_id'].dt.year
        records = json.loads(process_time.to_json(orient='records'))
        try:
            query = DimDate.insert_many(records)
            query = query.on_conflict(conflict_target=[DimDate.date_id,], update={
                "date_actual": peewee.EXCLUDED.date_actual,
                "day_of_week": peewee.EXCLUDED.day_of_week,
                "day_of_month": peewee.EXCLUDED.day_of_month,
                "month": peewee.EXCLUDED.month,
                "quarter": peewee.EXCLUDED.quarter,
                "year": peewee.EXCLUDED.year,
            })
            query.execute()
            get_logger().debug(f"load_date: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    
    def load_to_account(self, accounts: pd.DataFrame):
        process_accs = accounts.drop_duplicates().copy()
        process_accs.columns = [
            *map(
                lambda col: "address" if "address" in col else "name",
                process_accs.columns
            )
        ]
        process_accs = process_accs.dropna(subset=['address'])
        process_accs = process_accs.drop_duplicates(subset=['address'], keep='first')
        records = json.loads(process_accs.to_json(orient='records'))
        try:
            query = DimAccount.insert_many(records)
            query = query.on_conflict(conflict_target=[DimAccount.address,], update={
                "name": peewee.EXCLUDED.name,
            })
            query.execute()
            get_logger().debug(f"load_account: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def load_to_chain(self, chain_infos: pd.DataFrame):
        polkadot_parachains = {"moonbeam", "astar"}
        kusama_parachains = {"shiden", "moonriver"}
        process_info = chain_infos.drop_duplicates().copy()
        process_info.loc[process_info['chain_slug'].isin(polkadot_parachains), 'relay_chain'] = 'polkadot'
        process_info.loc[process_info['chain_slug'].isin(kusama_parachains), 'relay_chain'] = 'kusama'
        records = json.loads(process_info.to_json(orient='records'))
        try:
            query = DimChain.insert_many(records)
            query = query.on_conflict(conflict_target=[DimChain.chain_slug,], update={
                "relay_chain": peewee.EXCLUDED.relay_chain,
            })
            query.execute()
            get_logger().debug(f"load_chain: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def load_to_collection(self, collections: pd.DataFrame):
        process_info = collections.drop_duplicates().copy()
        records = json.loads(process_info.to_json(orient='records'))
        try:
            query = DimCollections.insert_many(records)
            query = query.on_conflict(conflict_target=[DimCollections.contract_address,], update={
                "contract_name": peewee.EXCLUDED.contract_name,
                "descriptions": peewee.EXCLUDED.descriptions,
                "owner_address": peewee.EXCLUDED.owner_address,
                "owner_name": peewee.EXCLUDED.owner_name,
                "link": peewee.EXCLUDED.link,
                "max_supply": peewee.EXCLUDED.max_supply,
                "total_supply": peewee.EXCLUDED.total_supply
            })
            query.execute()
            get_logger().debug(f"load_collections: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def load_to_activities(self, records: pd.DataFrame):
        process_records = json.loads(records.to_json(orient='records'))
        try:
            query = FactActivities.insert_many(process_records)
            query = query.on_conflict(
                conflict_target=[
                    FactActivities.date_id,
                    FactActivities.contract_address,
                    FactActivities.buyer_address,
                    FactActivities.seller_address,
                    FactActivities.chain_slug,],
                update={
                    "type_activities": peewee.EXCLUDED.type_activities,
                    "token_id": peewee.EXCLUDED.token_id,
                    "value": peewee.EXCLUDED.value,
                    "unit_usd": peewee.EXCLUDED.unit_usd,
                    "usd_value": peewee.EXCLUDED.usd_value,
                    "source_record": peewee.EXCLUDED.source_record,
                    "tx": peewee.EXCLUDED.tx
                })
            query.execute()
            get_logger().debug(f"load_activities: upsert {len(process_records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
    




