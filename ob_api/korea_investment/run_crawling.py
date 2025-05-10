from ob_api.korea_investment.api_helper import PeriodPriceHelper
from ob_api.korea_investment.api_util import get_api_token
from datetime import date
from ob_api.korea_investment.api_enums import PeriodType


if __name__ == "__main__":
    act = get_api_token()
    pph = PeriodPriceHelper(act)
    pph.call_api(stock_code='005930', start=date(year=2024, month=3, day=3),
                 end=date(2025, 3, 3), period=PeriodType.D)
