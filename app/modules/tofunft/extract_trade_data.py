from core.base_extractor import BaseExtractor
from core.core import get_logger
from .models import TradeData

from bs4 import BeautifulSoup
import traceback
import json
import datetime
import urllib.request as url_req


class TofuNftDataExtractor(BaseExtractor):

    network = {"shiden": 336, "astar": 592,
               "moonbeam": 1284}
            #    "moonbeam": 1284, "moonriver": 1285}

    def fetch(self, network: str,
              start_time: datetime.datetime = datetime.datetime.now() - datetime.timedelta(days=1),
              end_time: datetime.datetime = datetime.datetime.now()):
        MAX_NUM_PAGE = 1000
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7',
        }
        id_network = self.network.get(network)
        fetch_data = []
        if id_network:
            url = f"https://tofunft.com/discover/activities?category=all&network={id_network}&page=%d"
            continue_crawl = True
            for num_page in range(1, MAX_NUM_PAGE + 1):
                if continue_crawl:
                    try:
                        r = url_req.Request(url=url % num_page, headers=headers)
                        resp = url_req.urlopen(r).read()
                    except Exception as e:
                        print(e)
                        print(str(id_network) + "---- page:" + str(num_page))
                        break
                    soup = BeautifulSoup(resp, "html.parser")
                    data = json.loads(
                        soup.find('script', type='application/json').text)
                    records = data.get("props", {}).get("pageProps", {}).get(
                        "searchResp", {}).get('data', [])
                    for record in records:
                        created_at = record.get('created_at')
                        if created_at:
                            time_created = datetime.datetime.strptime(
                                created_at[:19], "%Y-%m-%dT%H:%M:%S")
                            if time_created <= end_time:
                                if time_created < start_time:
                                    continue_crawl = False
                                    break
                                appn_rec = record.copy()
                                appn_rec.update(
                                    {
                                        "created_at": time_created.strftime("%Y-%m-%d %H:%M:%S")
                                    }
                                )
                                fetch_data.append(appn_rec)
        return fetch_data

    def extract(self):
        chains = self.network
        extract_data = []
        for chain in chains.keys():
            fetch_chain = self.fetch(chain)
            for record in fetch_chain:
                extract_data.append(
                    {
                        "id_rec": record.get("id"),
                        "tx": record.get("tx"),
                        "category": record.get("category"),
                        "type_tx": record.get("type"),
                        "price": float((record.get("price", 0) or 0)),
                        "to_user_nickname": (record.get("to_user", {}) or {})\
                                            .get("nickname"),
                        "to_address": record.get("to_address"),
                        "from_user_nickname": (record.get("from_user", {}) or {})\
                                            .get("nickname"),
                        "from_address": record.get("from_address"),
                        "nft_name": ((record.get("nft", {}) or {})\
                                    .get("meta", {}) or {})\
                                    .get("name"),
                        "nft_description": ((record.get("nft", {}) or {})\
                                            .get("meta", {}) or {})\
                                            .get("description"),
                        "token_id": (record.get("nft", {}) or {})\
                                    .get("token_id"),
                        "nft_contract": (record.get("nft", {}) or {})\
                                        .get("nft_contract"),
                        "contract_name": ((record.get("nft", {}) or {})\
                                         .get("contract", {}) or {})\
                                         .get("name"),
                        "owner": (record.get("nft", {}) or {})\
                                .get("owner"),
                        "created_at": record.get("created_at"),
                        "network": chain
                    }
                )
        
        return extract_data
    
    def init_table(self):
        ## Create table if not exists
        try:
            if not TradeData.table_exists():
                TradeData.create_table(safe=True)
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def insert(self, record: dict):
        get_logger().debug("insert " + json.dumps(record))
        try:
            TradeData.create(
                id_rec=record.get("id_rec"),
                tx=record.get("tx"),
                category=record.get("category"),
                type_tx=record.get("type_tx"),
                price=record.get("price"),
                to_user_nickname=record.get("to_user_nickname"),
                to_address=record.get("to_address"),
                from_user_nickname=record.get("from_user_nickname"),
                from_address=record.get("from_address"),
                nft_name=record.get("nft_name"),
                nft_description=record.get("nft_description"),
                token_id=record.get("token_id"),
                nft_contract=record.get("nft_contract"),
                contract_name=record.get("contract_name"),
                owner=record.get("owner"),
                created_at=record.get("created_at"),
                network=record.get("network")
            )
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def update(self, record: dict):
        get_logger().debug("update " + json.dumps(record))
        id_rec = record.get("id_rec")
        try:
            update_query = TradeData\
                .update(record)\
                .where(TradeData.id_rec == id_rec)
            update_query.execute()
        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()

    def exist(self, record: dict):
        record_exists = False
        id_rec = record.get("id_rec", None)
        if id_rec:
            record_exists = TradeData\
                .select("id_rec")\
                .where(TradeData.id_rec == id_rec)\
                .exists()
        else:
            raise ValueError("id_rec is empty!!")
        return record_exists
