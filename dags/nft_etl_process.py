from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#pricing
from modules.santiment.extract import SantimentExtractor

#nft
from modules.moonbeans_io.extract_collection_data import NftCollectionDataExtractor
from modules.moonbeans_io.extract_trade_data import MoonbeansTradeDataExtractor
from modules.moonbeans_io.transform_load import MoonbeansTransformer

from modules.nftrade.extract_trade_data import NftradeDataExtractor
from modules.nftrade.transform_load import NftradeTransformer

from modules.tofunft.extract_trade_data import TofuNftDataExtractor
from modules.tofunft.transform_load import TofunftTransformer


def extract_santiment_price():
    extractor = SantimentExtractor()
    return extractor.process()

def extract_moonbeans_io_collection():
    extractor = NftCollectionDataExtractor()
    return extractor.process()

def extract_moonbeans_io_trades():
    extractor = MoonbeansTradeDataExtractor()
    return extractor.process()

def transform_moonbeans_io():
    extractor = MoonbeansTransformer()
    return extractor.process()

def extract_nftrade():
    extractor = NftradeDataExtractor()
    return extractor.process()

def transform_nftrade():
    extractor = NftradeTransformer()
    return extractor.process()

def extract_tofunft():
    extractor = TofuNftDataExtractor()
    return extractor.process()

def transform_tofunft():
    extractor = TofunftTransformer()
    return extractor.process()

args = {
    'owner': 'hadat'
}
with DAG(
        dag_id = 'NFT_pipeline',
        default_args = args,
        schedule_interval = "15 2 * * *",
        tags = ['final_thesis'],
        start_date = (datetime.today() - timedelta(days = 1)),
        concurrency = 2,
        max_active_runs = 4,
        params = {},
        catchup=False
) as dag:
    
    ## Extract
    
    extract_santiment_price = PythonOperator(
        task_id="extract_santiment_price",
        python_callable=extract_santiment_price
    )

    extract_moonbeans_io_collection = PythonOperator(
        task_id="extract_moonbeans_io_collection",
        python_callable=extract_moonbeans_io_collection
    )
    extract_moonbeans_io_trades = PythonOperator(
        task_id="extract_moonbeans_io_trades",
        python_callable=extract_moonbeans_io_trades
    )
    transform_moonbeans_io = PythonOperator(
        task_id="transform_moonbeans_io",
        python_callable=transform_moonbeans_io
    )


    extract_nftrade = PythonOperator(
        task_id="extract_nftrade",
        python_callable=extract_nftrade
    )
    transform_nftrade = PythonOperator(
        task_id="transform_nftrade",
        python_callable=transform_nftrade
    )

    extract_tofunft = PythonOperator(
        task_id="extract_tofunft",
        python_callable=extract_tofunft
    )
    transform_tofunft = PythonOperator(
        task_id="transform_tofunft",
        python_callable=transform_tofunft
    )

    # flow

    extract_santiment_price >> [extract_moonbeans_io_collection >> extract_moonbeans_io_trades,
                                extract_nftrade,
                                extract_tofunft] >> transform_nftrade >> transform_tofunft >> transform_moonbeans_io

    # extract_moonbeans_io_collection  

