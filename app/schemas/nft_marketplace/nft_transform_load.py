from .dims.account import DimAccount
from .dims.date import DimDate
from .dims.chain import DimChain
from .dims.collections import DimCollections, DimNFT
from .dims.source_record import DimSource
from .dims.types_activities import DimTypeActivity
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
    """Class Define task for ETL data from raw_area to Datamart NFT's Marketplace

    Args:
        BaseTransformLoad (_type_): Abstract Class define process to ETL from raw_area to dw_area 

    In each source from raw_area, it would use this class to implement process 
    for LOADING-data to datamart NFT's Marketplace in dw_area
    """

    # Specify time to run ETL process from raw_area to dw_area (or it can be seen as how we'll query new records in each table manually)
    # START_TIME = datetime.datetime.now()-datetime.timedelta(days=2)
    START_TIME = datetime.datetime(2022, 1, 18)
    # END_TIME = datetime.datetime.now()
    END_TIME = datetime.datetime.now()

    FORMAT_TIME_MAPPING = "%Y-%m-%d %H"

    def get_data_price(self, chains: list) -> pd.DataFrame:
        """Query data price in each chain to mapping each NFT's transaction

        Args:
            chains (list): speacify all chain data needs

        Returns:
            pd.DataFrame: frame contains hourly price in each token
        """
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

    def fill_transform_records(self, records: pd.DataFrame) -> pd.DataFrame:
        """Preprocessing task: CLean and fill Null value if records dont have to LOAD-process for DW

        A record must have all listed columns below

            * date_id: (for dim_date and fact_activities)

            * seller_address, seller_name: (for dim_account and fact_activities)

            * buyer_address, buyer_name: (for dim_account and fact_activities)

            * chain_slug: (for dim_chain, dim_collections, dim_activities)

            * contract_address (for dim_collections, fact_activities)
            
            * contract_name, contract_descriptions, owner_address, owner_name, link, max_supply (for dim_collections)

            * token_id (for dim_nft, fact_activities)
            
            * token_unique_name, token_images, token_descriptions (for dim_nft)

            * type_activities (for dim_type_act, fact_activities)

            * source_record (for dim_source, fact_activities)

            * value, unit_usd, usd_value, source_record, tx (for fact_activities)

        If a record don't have one of these columns, fill None to this field


        Args:
            records (pd.DataFrame): Transformed record in each source

        Returns:
            pd.DataFrame: records has enough columns and ready to 
        """

        FIXED_COLUMNS = {'date_id',\
                         'seller_address', 'seller_name',\
                         'buyer_address', 'buyer_name',\
                         'chain_slug',\
                         'contract_address', 'contract_name', 'contract_descriptions', 'owner_address', 'owner_name', 'link', 'max_supply',\
                         'token_id', 'token_unique_name', 'token_images', 'token_descriptions',\
                         'type_activities',\
                         'source_record',\
                         'value', 'unit_usd', 'usd_value', 'source_record', 'tx'}
        fill_columns = FIXED_COLUMNS.difference(set(records.columns))
        for field in fill_columns:
            records[field] = None
        

        return records

        

    def init_table(self):
        """Create table if not exists
        """
        try:
            if not DimAccount.table_exists():
                DimAccount.create_table(safe=True)
            if not DimDate.table_exists():
                DimDate.create_table(safe=True)
            if not DimChain.table_exists():
                DimChain.create_table(safe=True)
            if not DimCollections.table_exists():
                DimCollections.create_table(safe=True)
            if not DimNFT.table_exists():
                DimNFT.create_table(safe=True)
            if not DimTypeActivity.table_exists():
                DimTypeActivity.create_table(safe=True)
            if not DimSource.table_exists():
                DimSource.create_table(safe=True)
            if not FactActivities.table_exists():
                FactActivities.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def transform(self) -> pd.DataFrame:
        """Transforma process in each source table

        Returns:
            pd.DataFrame: transformed data
        """
        pass

    def bulk_load(self, records: pd.DataFrame):
        """LOADING task to datamart NFT's Marketplace

        Args:
            records (pd.DataFrame): _description_
        """
        records = records.sort_values('date_id', ascending=False)
        records = self.fill_transform_records(records)

        self.load_to_date(records[['date_id']])

        self.load_to_account(records[['seller_address', 'seller_name']])

        self.load_to_account(records[['buyer_address', 'buyer_name']])
        self.load_to_chain(records[['chain_slug']])

        self.load_to_collection(records[['contract_address', 'chain_slug', 'contract_name', 'contract_descriptions',\
                                         'owner_address',  'owner_name', 'link', 'max_supply']])
        self.load_to_nft(records[['contract_address', 'chain_slug', 'token_id', 'token_unique_name',\
                                  'token_images','token_descriptions']])
        
        self.load_to_type_activities(records[["type_activities"]])

        self.load_to_source_record(records[["source_record"]])

        self.load_to_activities(records[["date_id", "contract_address", "buyer_address", "seller_address",\
                                         "chain_slug", "type_activities", "token_id", "value", "unit_usd",\
                                         "usd_value", "source_record", "tx"]])
        pass

    def load_to_date(self, date_ids: pd.DataFrame):
        """LOADING task: Load to dim_date

        Args:
            date_ids (pd.DataFrame): dataframe contain data of timestamp and some essential field time need to load
        """
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
        """LOADING task: Load to dim_account

        Args:
            accounts (pd.DataFrame): Contain information of buyer/seller
        """
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
        """LOADING task: Load to chain

        Args:
            chain_infos (pd.DataFrame): contain chain_slugs for each chain
        """
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
        """LOADING task: load to dim_collection"""

        process_info = collections.drop_duplicates().copy()
        process_info["contract_with_chain"] = process_info['chain_slug'] + "-" + process_info["contract_address"]
        records = json.loads(process_info.to_json(orient='records'))
        try:
            query = DimCollections.insert_many(records)
            query = query.on_conflict(conflict_target=[DimCollections.contract_address, DimCollections.chain_slug], update={
                "contract_name": peewee.EXCLUDED.contract_name,
                "contract_descriptions": peewee.EXCLUDED.contract_descriptions,
                "owner_address": peewee.EXCLUDED.owner_address,
                "owner_name": peewee.EXCLUDED.owner_name
            })
            query.execute()
            get_logger().debug(f"load_collections: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
        
    def load_to_nft(self, nfts: pd.DataFrame):
        process_nft = nfts.drop_duplicates().copy()
        process_nft["contract_with_chain"] = process_nft['chain_slug'] + "-" + process_nft["contract_address"]
        process_nft["nft_id"] = process_nft['contract_with_chain'] + "-" + process_nft['token_id']
        process_nft = process_nft[['nft_id', 'contract_with_chain', 'token_id',\
                                   'token_unique_name', 'token_images', 'token_descriptions']]
        records = json.loads(process_nft.to_json(orient='records'))
        try:
            query = DimNFT.insert_many(records)
            query = query.on_conflict_ignore()
            query.execute()
            get_logger().debug(f"load_nft: upsert {len(records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
        
    
    def load_to_type_activities(self, type_activities: pd.DataFrame):

        process_acts = type_activities.drop_duplicates().copy()
        process_acts = json.loads(process_acts.to_json(orient="records"))
        try:
            query = DimTypeActivity.insert_many(process_acts)
            query = query.on_conflict_ignore()
            query.execute()
            get_logger().debug(f"load_type_activities: upsert {len(process_acts)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
    
    def load_to_source_record(self, source_record: pd.DataFrame):
        process_src = source_record.drop_duplicates().copy()
        process_src = json.loads(process_src.to_json(orient="records"))
        try:
            query = DimSource.insert_many(process_src)
            query = query.on_conflict_ignore()
            query.execute()
            get_logger().debug(f"load_source: upsert {len(process_src)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()


    def load_to_activities(self, records: pd.DataFrame):
        """LOADING task: Load to fact_activities

        Args:
            records (pd.DataFrame): record contains data for fact_activities for load process
        """
        process_records = records.drop_duplicates().copy()

        ## Find foreignkey in DimNFT
        process_records["contract_with_chain"] = process_records['chain_slug'] + "-" + process_records["contract_address"]
        process_records['nft_id'] = process_records['contract_with_chain'] + "-" + process_records['token_id']
        del process_records['contract_address']
        del process_records['chain_slug']
        del process_records['contract_with_chain']
        del process_records['token_id']

        ## Find foreignkey in DimTypeActivity
        tab_acts = pd.DataFrame(
            DimTypeActivity.select().dicts()
        )
        process_records = process_records.merge(tab_acts, on='type_activities', how='left')
        del process_records['type_activities']
        process_records = process_records.rename(columns={"id_type": "type_activities"})

        ## Find foreignkey in DimSource
        tab_src = pd.DataFrame(
            DimSource.select().dicts()
        )
        process_records = process_records.merge(tab_src, on='source_record', how='left')
        del process_records['source_record']
        process_records = process_records.rename(columns={"id_source": "source_record"})

        process_records = json.loads(process_records.to_json(orient='records'))
        try:
            query = FactActivities.insert_many(process_records)
            query = query.on_conflict(
                conflict_target=[
                    FactActivities.date_id,
                    FactActivities.buyer_address,
                    FactActivities.seller_address,
                    FactActivities.nft_id,
                    FactActivities.type_activities,
                    FactActivities.source_record],
                update={
                    "value": peewee.EXCLUDED.value,
                    "unit_usd": peewee.EXCLUDED.unit_usd,
                    "usd_value": peewee.EXCLUDED.usd_value,
                    "tx": peewee.EXCLUDED.tx
                })
            query.execute()
            get_logger().debug(f"load_activities: upsert {len(process_records)}")
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

        # return process_records
    




