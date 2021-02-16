from const import get_ticker_coordinates, get_hash_cache
from const import DIR, SDATE, CONFIG, logger, ENGINE
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
from hashlib import md5
import tarfile as tar
import pandas as pd
import sys, os
import json

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket, request

###################################################################################################

URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"

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
			'source' : 'cnbc'
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
		hash_cache[0].append(_hash)

		item['acquisition_datetime'] = datetime.now().isoformat()[:19]
		items.append(item)

	return items

def get_news(tickers, hash_cache, hashs):

	items = []
	N = len(tickers)
	for i, ticker in enumerate(tickers):

		progress = round(i / N * 100, 2)
		logger.info(f"collecting cnbc news, {ticker}, {progress}%")
		items.extend(fetch(ticker, hash_cache, hashs))

	return items

def save(items, hash_cache):

	json_file = Path(f"{DIR}/news_data/cnbc/{SDATE}.json")
	xz_file = json_file.with_suffix(".tar.xz")

	with open(f"{DIR}/data/cnbc_hash_cache.json", "w") as file:
		file.write(json.dumps(hash_cache))

	with open(json_file, "w") as file:
		file.write(json.dumps(items))

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(json_file, arcname=json_file.name)

	send_to_bucket("cnbc",
				   CONFIG['GCP']['RAW_BUCKET'],
				   xz_file.name,
				   xz_file.parent,
				   logger)

	# os.unlink(json_file)
	os.unlink(xz_file)

def main():

	tickers = get_ticker_coordinates()
	tickers = tickers.ticker.values.tolist()
	hash_cache, hashs = get_hash_cache('cnbc')

	items = get_news(tickers, hash_cache, hashs)
	save(items, hash_cache)

if __name__ == '__main__':

	logger.info("cnbc job, initializing")

	try:

		main()
		send_metric(CONFIG, "cnbc_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"cnbc job error, {e}")
		send_metric(CONFIG, "cnbc_success_indicator", "int64_value", 1)

	logger.info("cnbc job, terminating")

