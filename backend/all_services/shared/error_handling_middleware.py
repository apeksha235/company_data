import requests
import logging
import time
from requests.exceptions import RequestException

logging.basicConfig(
    filename="middleware.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


proxy = "http://sp76y0ei7t:kelp123@all.dc.smartproxy.com:10000"
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
class ErrorHandlingMiddleware:
    def __init__(self, retries=3, delay=5):
        self.retries = retries
        self.delay = delay

    def process_request(self,url):
        for i in range(self.retries):
            try:
                response=response = requests.get(url, proxies={"http": proxy, "https": proxy}, headers=headers)
                if response.status_code == 200:
                    return response.text
                else:
                    logging.warning("Request failed. Status Code: {}. Retry: {}".format(response.status_code, i+1))
            except RequestException as e:
                logging.warning("Request failed. Error: {}. Retry: {}".format(str(e), i+1))
            time.sleep(self.delay)

        logging.critical("Request failed after all retries.")
        return None
