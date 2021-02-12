from const import get_ticker_coordinates, get_hash_cache
from const import DIR, SDATE, CONFIG, ENGINE, logger
from pathlib import Path
from hashlib import md5
import tarfile as tar
import pandas as pd
import feedparser
import sys, os
import json
import time

import socket
socket.setdefaulttimeout(15)

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket

###################################################################################################

URL = "https://news.google.com/rss/search?q={query}+when:7d&hl=en-CA&gl=CA&ceid=CA:en"
news_sources = pd.read_csv(f"{DIR}/data/news_sources.csv")

###################################################################################################

def fetch(query, hash_cache, hashs):

	url = URL.format(query = query.replace(' ', '+'))
	items = feedparser.parse(url)

	cleaned_items = []
	for item in items['entries']:

		cleaned_item = {}

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

		author = item.get('source', {})
		author = author.get('title')
		if author:
			cleaned_item['author'] = author

		source_href = item.get("source", {})
		source_href = source_href.get("href")
		if source_href:
			cleaned_item['source_href'] = source_href

		cleaned_item['search_query'] = query
		cleaned_item['source'] = 'google'

		_hash = md5(json.dumps(cleaned_item).encode()).hexdigest()
		if _hash in hashs:
			continue

		hashs.add(_hash)
		hash_cache[0].append(_hash)
		cleaned_items.append(cleaned_item)

	return cleaned_items

def collect_news(ticker_coordinates, hash_cache, hashs):

	items = []
	N = len(ticker_coordinates)
	for i, data in enumerate(ticker_coordinates.values[:15]):

		queries = ' '.join(data)
		progress = round(i / N * 100, 2)
		logger.info(f"collecting google news, {queries}, {progress}%")

		ticker, company_name = data
		items.extend(fetch(ticker, hash_cache, hashs))
		items.extend(fetch(company_name, hash_cache, hashs))

	return items, hash_cache

def save(items, hash_cache):

	json_file = Path(f"{DIR}/news_data/google/{SDATE}.json")
	xz_file = json_file.with_suffix(".tar.xz")

	with open(f"{DIR}/data/google_hash_cache.json", "w") as file:
		file.write(json.dumps(hash_cache))

	with open(json_file, "w") as file:
		file.write(json.dumps(items))

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(json_file, arcname=json_file.name)

	send_to_bucket("google",
				   CONFIG['GCP']['RAW_BUCKET'],
				   xz_file.name,
				   xz_file.parent,
				   logger)

	# os.unlink(json_file)
	os.unlink(xz_file)

def main():

	ticker_coordinates = get_ticker_coordinates()
	hash_cache, hashs = get_hash_cache('google')

	items, hash_cache = collect_news(ticker_coordinates, hash_cache, hashs)	
	save(items, hash_cache)

if __name__ == '__main__':

	logger.info("google job, initializing")

	try:

		main()
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"google job error, {e}")
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)

	logger.info("google job, terminating")