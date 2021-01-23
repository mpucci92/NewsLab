from dummy_logger import DummyLogger
import requests
import time

user_agent = """Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1"""
headers_mobile = {'User-Agent' : user_agent}

def request(CONFIG, url, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	request_attempts = 0

	while request_attempts < max_tries:

		try:
			return requests.get(url, headers = headers_mobile, timeout = CONFIG['timeout'])
		except Exception as e:
			logger.warning(f"Request Error,{request_attempts},{e}")
		request_attempts += 1
		time.sleep(1)

	if request_attempts >=  max_tries:
		raise Exception("Max Request Attempts Reached.")
