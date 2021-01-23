from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET

from pathlib import Path
import sys, os
import json

def get_search(item):

	search = []

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

	return search

def rss():

	def transform(item):

		source = item['_source']
		source['authors'] = [
			author.lower()
			for author in
			source['authors'] + [source['article_source']]
		]
		source['categories'] = [
			cat.lower()
			for cat in
			source['categories']
		]
		source['abs_sentiment_score'] = abs(source['sentiment_score'])
		
		item['_source'] = source
		item = get_search(item)
		item['_index'] = "news"

		return item

	for file in (RSS_FOLDER / "old").iterdir():

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

		source['abs_sentiment_score'] = abs(source['sentiment_score'])
		item['_source'] = get_search(item['_source'])
		item['_index'] = "news"

		return item

	for file in (CNBC_FOLDER / "old").iterdir():

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
		item['_source'] = get_search(item['_source'])
		item['_index'] = "news"
		
		return item

	for file in (GOOGLE_FOLDER / "old").iterdir():

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

	rss()
	cnbc()
	google()