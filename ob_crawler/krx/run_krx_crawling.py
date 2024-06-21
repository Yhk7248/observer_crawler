import argparse
from ob_crawler.krx.krx_stock_crawler import KrxStockPriceCrawler
# from ob_crawler.krx.krx_index_crawler import KrxIndexPriceCrawler


def start_crawler(cr_type):
    if cr_type == "price":
        cr = KrxStockPriceCrawler()
        cr.prepare_crawling()

        for try_num in range(100):
            cmd = cr.kafka_topic_manager.receive_message()
            cr_data = cr.run_crawling(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cr-type", required=True, dest="cr_type", help="price / index")
    args = parser.parse_args()

    start_crawler(args.cr_type)
