from const import get_ticker_coordinates, get_hash_cache, save
from const import DIR, SDATE, CONFIG, logger
from datetime import datetime
from pathlib import Path
from hashlib import md5
import pandas as pd
import feedparser
import sys, os
import json
import time
import uuid

import socket
socket.setdefaulttimeout(15)

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket

###################################################################################################

URL = "https://news.google.com/rss/search?q={query}+when:7d&hl=en-CA&gl=CA&ceid=CA:en"
news_sources = list(pd.read_csv(f"{DIR}/data/news_sources.csv").news_source)
PATH = Path(f"{DIR}/news_data/google/")

###################################################################################################

def fetch(query, hash_cache, hashs):

	url = URL.format(query = query.replace(' ', '+'))
	items = feedparser.parse(url)

	cleaned_items = []
	for item in items['entries']:

		cleaned_item = {
			'search_query' : query,
			'_source' : 'google'
		}

		title = item.get('title')
		if title:
			cleaned_item['title'] = title

		link = item.get('link')
		if link:
			cleaned_item['link'] = link

		published = item.get('published')
		if published:
			cleaned_item['published'] = published

		published_parsed = item.get('published_parsed')
		if published_parsed:
			iso = time.strftime('%Y-%m-%dT%H:%M:%S', published_parsed)
			cleaned_item['published_parsed'] = iso

		article_source = item.get('source', {})
		article_source = article_source.get('title')
		if article_source:
			cleaned_item['article_source'] = article_source

		if article_source not in news_sources:
			continue

		source_href = item.get("source", {})
		source_href = source_href.get("href")
		if source_href:
			cleaned_item['source_href'] = source_href

		_hash = md5(json.dumps(cleaned_item).encode()).hexdigest()
		if _hash in hashs:
			continue

		hashs.add(_hash)
		hash_cache[0].append(_hash)

		cleaned_item['acquisition_datetime'] = datetime.now().isoformat()[:19]
		cleaned_items.append(cleaned_item)

	if len(cleaned_items) == 0:
		return

	fname = str(uuid.uuid4())
	with open(PATH / f"{fname}.json", "w") as file:
		file.write(json.dumps(cleaned_items))

def collect_news(ticker_coordinates, hash_cache, hashs):

	N = len(ticker_coordinates)
	for i, data in enumerate(ticker_coordinates.values):

		queries = ' '.join(data)
		progress = round(i / N * 100, 2)
		logger.info(f"collecting google news, {queries}, {progress}%")

		ticker, company_name = data
		fetch(ticker, hash_cache, hashs)
		fetch(company_name, hash_cache, hashs)

def main():

	ticker_coordinates = get_ticker_coordinates()
	hash_cache, hashs = get_hash_cache('google')
	collect_news(ticker_coordinates, hash_cache, hashs)	
	save('google', PATH, hash_cache, send_to_bucket, send_metric)

if __name__ == '__main__':

	logger.info("google job, initializing")

	try:

		main()
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"google job error, {e}")
		send_metric(CONFIG, "google_success_indicator", "int64_value", 0)

	logger.info("google job, terminating")