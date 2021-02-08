from const import DIR, SDATE, CONFIG, ENGINE
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

def get_ticker_coordinates():

	try:

		df = pd.read_sql("""
			SELECT
				*
			FROM
				instruments
			ORDER BY
				market_cap
				DESC
		""", ENGINE)

		df.to_csv(f"{DIR}/data/ticker_coordinates.csv", index=False)

	except Exception as e:

		print(e)
		df = pd.read_csv(f"{DIR}/data/ticker_coordinates.csv")

	return df[['ticker', 'name']]

def get_hash_cache():

	try:

		with open(f"{DIR}/data/hash_cache.json", "r") as file:
			hash_cache = json.loads(file.read())

		hash_cache = {
			int(key) + 1 : hash_cache[key]
			for key in hash_cache
		}
		del hash_cache[7]
		hash_cache[0] = []

	except Exception as e:

		print(e)
		hash_cache = {
			i : []
			for i in range(7)
		}

	hashs = set([
		_hash
		for hash_list in hash_cache.values()
		for _hash in hash_list
	])

	return hash_cache, hashs

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
	for i, data in enumerate(ticker_coordinates.values):

		print(f"Querying for: {data}. Progress: {round(i / N * 100, 2)}%")

		ticker, company_name = data
		items.extend(fetch(ticker, hash_cache, hashs))
		items.extend(fetch(company_name, hash_cache, hashs))

	return items, hash_cache

def save(items, hash_cache):

	json_file = Path(f"{DIR}/news_data/{SDATE}.json")
	xz_file = json_file.with_suffix(".tar.xz")

	with open(f"{DIR}/data/hash_cache.json", "w") as file:
		file.write(json.dumps(hash_cache))

	with open(json_file, "w") as file:
		file.write(json.dumps(items))

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(json_file, arcname=json_file.name)

	send_to_bucket("google", CONFIG['GCP']['RAW_BUCKET'], xz_file.name, xz_file.parent)

	os.unlink(json_file)
	os.unlink(xz_file)

def main():

	ticker_coordinates = get_ticker_coordinates()
	hash_cache, hashs = get_hash_cache()

	items, hash_cache = collect_news(ticker_coordinates, hash_cache, hashs)
	save(items, hash_cache)

if __name__ == '__main__':

	try:

		main()
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)

	except Exception as e:

		print(e)
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)
