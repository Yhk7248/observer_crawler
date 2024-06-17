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
        self.open_krx()
        self.open(self.url)
        self.wait_until_disabled(By.CLASS_NAME, "loading-bar-wrap")

        driver_last_request = [req for req in self.driver.requests if "getJsonData.cmd" in str(req)][-1]
        cookies = self.driver.get_cookies()
        header = dict(driver_last_request.headers)
        form_url_decoded = driver_last_request.body.decode('utf-8')
        form_param_dict = flatten_dict(urllib.parse.parse_qs(form_url_decoded))

        self.http_manager = KrxHttp(
            cookies=cookies, header=header,
            proxy=self.proxy, form_url_dict=form_param_dict,
            url="http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        )

    def parse_data(self, response_json: dict):
        data_list = response_json['OutBlock_1']
        parsed = []
        for data in data_list:
            parsed.append(KrxPriceData(**data))

        return parsed

    def run_crawling(self, date: str):
        self.kafka_topic_manager.receive_message()
        response_json = self.http_manager.post(date)
        return self.parse_data(response_json)
