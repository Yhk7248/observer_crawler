import argparse
from ob_crawler.krx.krx_stock_crawler import KrxStockPriceCrawler
# from ob_crawler.krx.krx_index_crawler import KrxIndexPriceCrawler


def start_crawler(cr_type):
    if cr_type == "price":
        cr = KrxStockPriceCrawler()
        cr.prepare_crawling()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cr-type", required=True, dest="cr_type", help="price or index")
    args = parser.parse_args()

    start_crawler(args.cr_type)
