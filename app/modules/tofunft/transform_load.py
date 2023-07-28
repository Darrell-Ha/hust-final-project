from schemas.nft_marketplace.nft_transform_load import NftTransformer
from .models import TofuTradeData
from core.core import get_logger
import pandas as pd


class TofunftTransformer(NftTransformer):
    
    def transform(self) -> pd.DataFrame:
        new_data = pd.DataFrame(
            TofuTradeData\
                .select()\
                .where(
                    TofuTradeData.created_at.between(self.START_TIME, self.END_TIME)
                )\
                .dicts()
        )
        data_price = self.get_data_price(chains=["shiden", "astar", "moonbeam"])
        data_price = data_price.rename(columns={"chain": "network"})
        new_data['time_price_mapping'] = new_data['created_at'].dt.strftime(self.FORMAT_TIME_MAPPING)
        process_data = new_data.merge(data_price, on=['network', 'time_price_mapping'], how='left')
        process_data['usd_value'] = process_data['unit_usd'] * process_data['price']
        process_data['source_record'] = 'tofunft'

        ## extract
        process_data = process_data.rename(
            columns={
                "created_at": "date_id",
                "from_address": "seller_address",
                "from_user_nickname": "seller_name",
                "to_address": "buyer_address",
                "to_user_nickname": "buyer_name",
                "network": "chain_slug",
                "nft_contract": "contract_address",
                "contract_name": "contract_name",
                "token_id": "token_id",
                "nft_name":"token_unique_name",
                "nft_description": "token_descriptions",
                "category": "type_activities",
                "source_record": "source_record",
                "price": "value",
                "unit_usd": "unit_usd",
                "usd_value": "usd_value",
                "tx": "tx"
            }
        )
        process_data = process_data[["date_id", "seller_address", "seller_name", "buyer_address", "buyer_name",\
                                     "chain_slug", "contract_address", "contract_name", "token_id",\
                                     "token_unique_name", "token_descriptions", "type_activities", "source_record",\
                                     "value", "unit_usd", "usd_value", "tx"]]
        
        process_data = process_data.drop_duplicates(subset=['date_id', 'buyer_address', 'seller_address', 'chain_slug', 'contract_address', 'token_id', 'type_activities', 'source_record'], keep='last')
        
        get_logger().debug(f"transformed: {len(process_data)}")


        return process_data

