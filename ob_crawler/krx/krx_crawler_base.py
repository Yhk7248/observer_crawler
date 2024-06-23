import abc

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.chrome import ChromeDriverManager

from ob_crawler.krx.krx_conf import BASE_URL
from ob_kafka.kafka_manager import KafkaManager


class KrxCrawlerBase(metaclass=abc.ABCMeta):
    """ base interface for crawling krx """
    def __init__(self, url, proxy, topic_name):
        self.url = url
        self.proxy = {} if proxy is None else proxy.get("proxy", {})
        self.browser_options = webdriver.ChromeOptions()
        self.service = Service(executable_path=ChromeDriverManager().install())
        self.generate_options()
        self.driver = self.generate_driver()
        self.kafka_topic_manager = KafkaManager(
            topic_name=topic_name, group_id='krx'
        )

    def generate_driver(self):
        if len(self.proxy):
            driver = webdriver.Chrome(service=self.service, options=self.browser_options,
                                      seleniumwire_options=self.proxy)
        else:
            driver = webdriver.Chrome(service=self.service, options=self.browser_options,)
        return driver

    def generate_options(self):
        self.browser_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.browser_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.browser_options.add_experimental_option("useAutomationExtension", False)
        self.browser_options.add_experimental_option("detach", True)
        # prefs = {
        #     "profile.managed_default_content_settings.images": 2,  # Disable images
        #     "profile.default_content_setting_values.fonts": 2,  # Disable fonts
        #     "profile.managed_default_content_settings.javascript": 2,  # Disable JavaScript
        #     "profile.managed_default_content_settings.stylesheets": 2  # Disable CSS
        # }
        # self.browser_options.add_experimental_option('prefs', prefs)
        self.browser_options.add_argument("--no-sandbox")  # No protection needed
        self.browser_options.add_argument("--headless=new")  # Hide the GUI
        self.browser_options.add_argument("--single-process")  # Lambda only give us only one CPU
        self.browser_options.add_argument("--disable-dev-shm-usage")
        self.browser_options.add_argument("--disable-extensions")  # disabling extensions
        self.browser_options.add_argument("--disable-gpu")  # applicable to windows os only
        self.browser_options.add_argument("--disable-infobars")  # disabling infobars

    def open_krx(self):
        self.open(BASE_URL)
        self.wait_until_located(By.ID, "jsMainLnbWrap")

    def open(self, url):
        self.driver.get(url)

    def close(self):
        self.driver.quit()
        self.driver.close()

    def wait_until_located(self, by, by_str: str, time_wait=10.0):
        try:
            element = WebDriverWait(driver=self.driver, timeout=time_wait).until(
                ec.presence_of_element_located((by, by_str))
            )
            return element
        except Exception as e:
            raise e

    def wait_until_disabled(self, by, by_str: str, time_wait=10.0):
        WebDriverWait(self.driver, time_wait).until(
            ec.invisibility_of_element_located((by, by_str))
        )

    @abc.abstractmethod
    def prepare_crawling(self):
        pass

    @abc.abstractmethod
    def parse_data(self, data):
        pass

    @abc.abstractmethod
    def run_crawling(self, date):
        pass
