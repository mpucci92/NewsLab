from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET

from pathlib import Path
import sys, os
import json

def get_search(item):

	search = []
	source = item['_source']

	title = source.get("title", "")
	if title:
		search.append(title)

	summary = source.get("summary", "")
	if summary:
		search.append(summary)

	categories = source.get("categories", "")
	if categories:
		search.extend(categories)

	if len(search) != 0:
		item['_source']['search'] = search
	elif 'search' in item['_source']:
		del item['_source']['search']

	return item

def rss():

	def transform(item):

		source = item['_source']

		if source.get("authors"):
			source['authors'] = [
				author.lower()
				for author in
				source['authors'] + [source['article_source']]
			]

		if source.get("categories"):
			source['categories'] = [
				cat.lower()
				for cat in
				source['categories']
			]

		tickers = source.get("tickers")
		if tickers:
			for ticker in tickers:
				if ':' in ticker:
					tickers.append(ticker.split(':')[1])
			source['tickers'] = list(set(tickers))

		source['abs_sentiment_score'] = abs(source['sentiment_score'])
		source['title'] = source['title'].strip()
		source['summary'] = source['summary'].strip()
		
		item['_source'] = source
		item = get_search(item)
		item['_index'] = "news"

		return item

	for file in sorted((RSS_FOLDER / "old").iterdir()):

		print("RSS:", file.name)

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		new_items = [
			transform(item)
			for item in items
		]

		with open(RSS_FOLDER / "new" / file.name, "w") as _file:
			_file.write(json.dumps(new_items))

def cnbc():

	def transform(item):

		if item['_source'].get("article_type"):
			item['_source']['article_type'] = item['_source']['article_type'].strip().lower()

		item['_source']['authors'] = item['_source']['authors'].strip().lower()
		if item['_source']['authors'] == "cnbc.com":
			item['_source']['authors'] = "cnbc"

		item['_source']['abs_sentiment_score'] = abs(item['_source']['sentiment_score'])
		item = get_search(item)
		item['_index'] = "news"

		tickers = item['_source'].get("tickers")
		if tickers:
			for ticker in tickers:
				if ":" in ticker:
					tickers.append(ticker.split(":")[1].strip())
			item['_source']['tickers'] = list(set(tickers))

		return item

	for file in sorted((CNBC_FOLDER / "old").iterdir()):

		print("CNBC:", file.name)

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		new_items = [
			transform(item)
			for item in items
		]

		with open(CNBC_FOLDER / "new" / file.name, "w") as _file:
			_file.write(json.dumps(new_items))

def google():

	def transform(item):

		item['_source']['abs_sentiment_score'] = abs(item['_source']['sentiment_score'])
		item['_source']['authors'] = item['_source']['authors'].strip().lower()
		item['_source']['title'] = item['_source']['title'].strip()
		item = get_search(item)
		item['_index'] = "news"

		tickers = item['_source'].get("tickers")
		if tickers:
			for ticker in tickers:
				if ":" in ticker:
					tickers.append(ticker.split(":")[1].strip())
			item['_source']['tickers'] = list(set(tickers))

		return item

	for file in sorted((GOOGLE_FOLDER / "old").iterdir()):

		print("GOOGLE:", file.name)

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		new_items = [
			transform(item)
			for item in items
		]

		with open(GOOGLE_FOLDER / "new" / file.name, "w") as _file:
			_file.write(json.dumps(new_items))

if __name__ == '__main__':

	google()
	cnbc()
	rss()