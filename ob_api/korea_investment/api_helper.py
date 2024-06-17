import abc
import json
import requests
from urllib.parse import urljoin
from datetime import date

from ob_api.korea_investment.api_conf import API_KEY, API_SECRET, real_domain
from ob_api.korea_investment.api_enums import PeriodType


class BaseHelper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def gen_header(self, **kwargs):
        pass

    @abc.abstractmethod
    def gen_param(self, **kwargs):
        pass

    @abc.abstractmethod
    def parse(self, **kwargs):
        pass

    @abc.abstractmethod
    def call_api(self, **kwargs):
        pass


class PeriodPriceHelper(BaseHelper):
    def __init__(self, authorization, real=False):
        self.end_point = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        self.auth = authorization
        self.real = real

    def gen_header(self):
        header = {
            "content-type": "application/json; charset=utf-8",  # content type
            "authorization": self.auth,  # 한국투자증권 auth API 로 받은 토큰
            "appkey": API_KEY,          # 한국투자증권 APP KEY
            "appsecret": API_SECRET,    # 한국투자증권 APP SECRET
            "tr_id": "FHKST03010100",   # 거래 ID(API 고유)
            "custtype": "P"  # B:법인, P: 개인
        }
        return header

    def gen_param(self, stock_code: str, start: date, end: date, period: PeriodType):
        param = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": end.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": period.value,
            "FID_ORG_ADJ_PRC": "1"
        }
        return param

    def parse(self):
        pass

    def call_api(self, stock_code: str, start: date, end: date, period: PeriodType):
        header = self.gen_header()
        param = self.gen_param(stock_code, start, end, period)
        url = urljoin(real_domain, self.end_point)

        response = requests.get(url, headers=header, data=json.dumps(param))

        if response.status_code == 200:
            print(response.json())


class TokenHelper(BaseHelper):

    def __init__(self, real=False):
        self.end_point = "/oauth2/tokenP"
        self.real = real

    def gen_header(self):
        header = {
            "content-type": "application/json"
        }
        return header

    def gen_param(self):
        param = {
            "grant_type": "client_credentials",
            "appkey": API_KEY,
            "appsecret": API_SECRET
        }
        return param

    def parse(self, response):
        return response.json()["access_token"]

    def call_api(self):
        header = self.gen_header()
        param = self.gen_param()
        url = urljoin(real_domain, self.end_point)

        response = requests.post(url, headers=header, data=json.dumps(param))

        if response.status_code == 200:
            access_token = self.parse(response)
            return access_token

        return None
