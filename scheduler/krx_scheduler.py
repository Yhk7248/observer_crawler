
import time
import schedule
from datetime import datetime, timedelta

from ob_db.ts_db import TsDb
from ob_util.log_util import printl
from ob_crawler.krx.run_krx_enq_kafka import run_enqueue_krx
from ob_crawler.krx.run_krx_crawling import start_crawler


def get_cr_day(cr_source='KRX'):
    ts_db = TsDb()
    ts_db.connect()
    cr_day = ts_db.get_ts_cr_day(cr_source)

    if cr_day is None:
        s_date = "19990101"
        return s_date

    printl(f'KRX crawling: {cr_day}')

    return cr_day['cr_date']


def upsert_cr_day(cr_date, cr_source='KRX'):
    ts_db = TsDb()
    ts_db.connect()
    ts_db.upsert_ts_cr_day(cr_date=cr_date, cr_source=cr_source)
    ts_db.close()


def job():
    s_date = get_cr_day()
    run_enqueue_krx(topic_name='stock_price', start_date=s_date)
    start_crawler('price')
    upsert_cr_day(cr_date=datetime.strptime(s_date, '%Y%m%d') + timedelta(days=100))


if __name__ == "__main__":
    job()

# schedule.every().day.at("22:30").do(job)      # for crontab
#
# while True:
#     schedule.run_pending()
#     time.sleep(3)

# TODO: PEP 녹이기
# PEP 8, 20, 257, 484/526, 572: 크롤링 코드의 품질, 유지보수, 협업 효율을 크게 높임
# PEP 556: 대용량 멀티스레드 크롤링에서 성능 개선에 참고할 만함
# PEP 249: DB 연동 크롤러라면 필수
