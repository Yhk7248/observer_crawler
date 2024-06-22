import time
import uuid
import argparse
import random
from datetime import datetime

from ob_db.ts_db import TsDb
from ob_db.dataclass import TsStock
from ob_crawler.krx.krx_dataclass import KrxPriceData
from ob_crawler.krx.krx_stock_crawler import KrxStockPriceCrawler
# from ob_crawler.krx.krx_index_crawler import KrxIndexPriceCrawler


def start_crawler(cr_type):
    if cr_type == "price":
        cr = KrxStockPriceCrawler()
        cr.prepare_crawling()

        for try_num in range(100):
            try:
                cmd = cr.kafka_topic_manager.receive_message()
                cr_data_list = cr.run_crawling(cmd)
                insert_cr_ts_data(cmd.get('date'), cr_data_list)
            except Exception as e:
                print(e)

            time.sleep(random.uniform(15, 20))


def insert_cr_ts_data(price_date, cr_data_list: list[KrxPriceData]):
    ts_db = TsDb()
    ts_db.connect()
    price_date = datetime.strptime(price_date, "%Y%m%d").date()
    for cr_data in cr_data_list:
        stock = TsStock(
            ts_id=str(uuid.uuid4()),
            stock_code=cr_data.ISU_SRT_CD,
            stock_name=cr_data.ISU_ABBRV,
            market_type=cr_data.MKT_NM,
            closing_price=convert_to_number(cr_data.TDD_CLSPRC),
            price_change=convert_to_number(cr_data.CMPPREVDD_PRC),
            price_change_rate=convert_to_number(cr_data.FLUC_RT),
            opening_price=convert_to_number(cr_data.TDD_OPNPRC),
            high_price=convert_to_number(cr_data.TDD_HGPRC),
            low_price=convert_to_number(cr_data.TDD_LWPRC),
            trade_volume=convert_to_number(cr_data.ACC_TRDVOL),
            trade_amount=convert_to_number(cr_data.ACC_TRDVAL),
            market_cap=convert_to_number(cr_data.MKTCAP),
            listed_shares=convert_to_number(cr_data.LIST_SHRS)
        )
        ts_db.insert_ts_stock(stock)
        ts_db.insert_ts_data(price_date, stock)

    ts_db.commit()
    ts_db.close()


def convert_to_number(number_str):
    number_str = number_str.replace(",", "")
    try:
        if '.' in number_str:
            return float(number_str)
        else:
            return int(number_str)
    except ValueError:
        print(f"Error: {number_str} is not a valid number.")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cr-type", required=True, dest="cr_type", help="price / index")
    args = parser.parse_args()

    start_crawler(args.cr_type)
