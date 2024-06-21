import json
import requests
import urllib.parse
from http.cookiejar import Cookie
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class KrxHttp:

    def __init__(
        self, cookies: list, header: dict,
        proxy: dict, url: str, body: dict,
        max_retry: int = 1
    ):
        self.header = header
        self.cookies = cookies
        self.proxy = proxy
        self.url = url
        self.body = body
        self.max_retry = max_retry
        self.session = requests.Session()
        self.status_code = None
        self.prepare_cookies()

    def prepare_cookies(self):
        self.set_adapter()
        for cookie in self.cookies:
            self.set_cookie(cookie)

    def set_adapter(self):
        retries = Retry(
            total=self.max_retry,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504, 522]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        if len(self.proxy):
            self.session.proxies.update(self.proxy)

    def set_header(self):
        self.session.headers.update(self.header)

    def set_cookie(self, cookie):
        if 'expiry' in cookie:
            cookie['expires'] = cookie['expiry']
            del cookie['expiry']

        c = Cookie(
            version=0,
            name=cookie.get('name'),
            value=cookie.get('value'),
            port=None,
            port_specified=False,
            domain=cookie.get('domain', ''),
            domain_specified=bool(cookie.get('domain')),
            domain_initial_dot=False,
            path=cookie.get('path', '/'),
            path_specified=bool(cookie.get('path')),
            secure=cookie.get('secure', False),
            expires=cookie.get('expires'),
            discard=False,
            comment=None,
            comment_url=None,
            rest={'HttpOnly': cookie.get('httpOnly', None)},
            rfc2109=False,
        )
        self.session.cookies.set_cookie(c)

    def post(self, date):
        self.body['trdDd'] = date
        form_url_encoded = urllib.parse.urlencode(self.body, doseq=True)

        response = self.session.post(
            url=self.url, data=form_url_encoded, headers=self.header
        )

        if response.status_code == 200:
            return json.loads(response.content.decode())
        else:
            raise Exception("response.status is not 200")
