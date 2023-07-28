from schemas.nft_marketplace.nft_transform_load import NftTransformer
from .models import MoonbeansCollectionData, MoonbeansTradeData

from core.core import get_logger
import pandas as pd


class MoonbeansTransformer(NftTransformer):
    
    def transform(self) -> pd.DataFrame:
        new_data = pd.DataFrame(
            MoonbeansTradeData\
                .select()\
                .where(
                    MoonbeansTradeData.timestamp.between(self.START_TIME, self.END_TIME)
                )\
                .dicts()
        )
        data_collections = pd.DataFrame(
            MoonbeansCollectionData.select(
                MoonbeansCollectionData.contractAddress,
                MoonbeansCollectionData.chain,
                MoonbeansCollectionData.title,
                MoonbeansCollectionData.owner,
                MoonbeansCollectionData.fullDescription,
                MoonbeansCollectionData.links,
                MoonbeansCollectionData.totalSupply,
                MoonbeansCollectionData.maxSupply,
            ).dicts()
        )

        new_data = new_data.merge(data_collections, on='contractAddress', how='left')
        data_price = self.get_data_price(chains=["moonbeam", "moonriver"])
        new_data['time_price_mapping'] = new_data['timestamp'].dt.strftime(self.FORMAT_TIME_MAPPING)
        process_data = new_data.merge(data_price, on=['chain', 'time_price_mapping'], how='left')
        process_data['usd_value'] = process_data['unit_usd'] * process_data['value']
        process_data['tokenId'] = process_data['tokenId'].astype(str)
        process_data['source_record'] = 'moonbeans.io'
        process_data["type_activities"] = "sale"
        

        ## extract
        process_data = process_data.rename(
            columns={
                "timestamp": "date_id",
                "seller": "seller_address",
                "buyer": "buyer_address",
                "chain": "chain_slug",
                "contractAddress": "contract_address",
                "title": "contract_name",
                "fullDescription": "contract_descriptions",
                "owner": "owner_name",
                "links": "link",
                "maxSupply": "max_supply",
                "type_activities": "type_activities",
                "source_record": "source_record",
                "tokenId": "token_id",
                "value": "value",
                "unit_usd": "unit_usd",
                "usd_value": "usd_value",
                "tx": "tx"
            }
        )
        process_data = process_data[["date_id","seller_address","buyer_address","chain_slug",\
                                     "contract_address","contract_name","contract_descriptions",\
                                     "owner_name","link","max_supply","type_activities","source_record",\
                                     "token_id","value","unit_usd","usd_value","tx"]]
        
        get_logger().debug(f"transformed: {len(process_data)}")

        return process_data
    
