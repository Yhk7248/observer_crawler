from ob_api.korea_investment.api_helper import PeriodPriceHelper
from ob_api.korea_investment.api_util import get_api_token


if __name__ == "__main__":
    act = get_api_token()
    pph = PeriodPriceHelper(act)
    pph.call_api()
