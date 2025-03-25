
import time
import schedule

from ob_db.ts_db import TsDb
from ob_util.log_util import printl
from ob_crawler.krx.run_krx_enq_kafka import run_enqueue_krx
from ob_crawler.krx.run_krx_crawling import start_crawler


def get_cr_day():
    ts_db = TsDb()
    ts_db.connect()
    cr_day = ts_db.get_ts_cr_day('KRX')
    printl(f'KRX crawling: {cr_day}')

    return cr_day['cr_date']


def job():
    s_date = get_cr_day()
    run_enqueue_krx(topic_name='stock_price', start_date=s_date)
    start_crawler('price')


schedule.every().day.at("22:30").do(job)

while True:
    schedule.run_pending()
    time.sleep(3)
