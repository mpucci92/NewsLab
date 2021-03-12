from helpers import get_ticker_coordinates, get_hash_cache, save
from const import DIR, SDATE, CONFIG, logger
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
from hashlib import md5
import sys, os
import json
import uuid

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket, request

###################################################################################################

URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"
PATH = Path(f"{DIR}/news_data/cnbc/")

###################################################################################################

def fetch(ticker, hash_cache, hashs):

	url = URL.format(ticker=ticker)
	page = request(url).content
	page = BeautifulSoup(page, features="lxml")

	items = []
	titles = page.find_all("div", {"class" : "LatestNews-headline"})
	timestamps = page.find_all("div", {"class" : "LatestNews-timestamp"})
	sources = page.find_all("span", {"class" : "LatestNews-source"})

	for title, timestamp, source in zip(titles, timestamps, sources):

		item = {
			'title' : title.text,
			'article_source' : source.text,
			'links' : [],
			'_source' : 'cnbc'
		}

		ts = timestamp.text
		if 'Ago' in ts:

			components = ts.split(' ')
			
			if 'Min' in ts:
				td = timedelta(minutes=float(components[0]))
			elif 'Hour' in ts: 
				td = timedelta(hours=float(components[0]))
			else:
				td = timedelta(minutes=0)

			item['published'] = (datetime.now() - td).isoformat()[:19]

		else:

			item['published'] = datetime.strptime(ts, '%B %d, %Y').isoformat()[:19]

		a_tags = title.find_all("a", href=True)
		for a_tag in a_tags:
			item['links'].append(a_tag['href'])

		_hash = md5(json.dumps(item).encode()).hexdigest()
		if _hash in hashs:
			continue

		hashs.add(_hash)
		hash_cache[SDATE].append(_hash)

		item['acquisition_datetime'] = datetime.now().isoformat()[:19]
		items.append(item)

	if len(items) == 0:
		return

	fname = str(uuid.uuid4())
	with open(PATH / f"{fname}.json", "w") as file:
		file.write(json.dumps(items))

def collect_news(tickers, hash_cache, hashs):

	N = len(tickers)
	for i, ticker in enumerate(tickers):

		progress = round(i / N * 100, 2)
		logger.info(f"collecting cnbc news, {ticker}, {progress}%")
		fetch(ticker, hash_cache, hashs)

def main():

	tickers = get_ticker_coordinates()
	tickers = tickers.ticker.values.tolist()
	hash_cache, hashs = get_hash_cache('cnbc')
	collect_news(tickers, hash_cache, hashs)
	save('cnbc', PATH, hash_cache, hashs, send_to_bucket, send_metric)

if __name__ == '__main__':

	logger.info("cnbc job, initializing")

	try:

		main()
		send_metric(CONFIG, "cnbc_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"cnbc job error, {e}")
		send_metric(CONFIG, "cnbc_success_indicator", "int64_value", 0)

	logger.info("cnbc job, terminating")

