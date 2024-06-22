import time
import urllib.parse

from selenium.webdriver.common.by import By

from ob_crawler.krx.krx_dataclass import KrxPriceData
from ob_crawler.krx.krx_http_helper import KrxHttp
from ob_crawler.krx.krx_crawler_base import KrxCrawlerBase
from ob_crawler.krx.krx_util import flatten_dict


class KrxStockPriceCrawler(KrxCrawlerBase):
    def __init__(self, proxy=None):
        url = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101"
        self.http_manager = None
        self.topic_name = 'stock_price'
        super().__init__(url, proxy, self.topic_name)

    def prepare_crawling(self):
        def get_target():
            for req in self.driver.requests:
                if "getJsonData.cmd" not in str(req):
                    continue
                body_str = req.body.decode('utf-8')
                t_body = flatten_dict(urllib.parse.parse_qs(body_str))
                if "bld" in t_body and t_body["bld"] == "dbms/MDC/STAT/standard/MDCSTAT01501":
                    b_cookies = self.driver.get_cookies()
                    t_header = dict(req.headers)
                    return b_cookies, t_header, t_body

        self.open_krx()
        time.sleep(10)
        self.open(self.url)
        time.sleep(10)
        self.wait_until_disabled(By.CLASS_NAME, "loading-bar-wrap")

        cookies, header, body = get_target()

        self.http_manager = KrxHttp(
            cookies=cookies, header=header,
            proxy=self.proxy, body=body,
            url="http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        )

    def parse_data(self, response_json: dict):
        data_list = response_json['OutBlock_1']
        parsed = []
        for data in data_list:
            parsed.append(KrxPriceData(**data))

        return parsed

    def run_crawling(self, cmd: dict):
        response_json = self.http_manager.post(cmd.get('date'))
        return self.parse_data(response_json)
