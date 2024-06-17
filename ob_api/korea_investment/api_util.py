from ob_api.korea_investment.api_helper import TokenHelper


def get_api_token():
    tk_helper = TokenHelper()
    access_token = tk_helper.call_api()
    return access_token
