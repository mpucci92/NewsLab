from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, SUBSET

from pathlib import Path
import sys, os
import json

def get_search(source):

	search = []

	title = source.get("title")
	if title:
		search.append(title.strip())

	summary = source.get("summary")
	if summary:
		search.append(summary.strip())

	source['search'] = search

	return source

def rss():

	def transform(item):
		item['_source'] = get_search(item['_source'])
		item['_index'] = "news"
		return item

	for file in (RSS_FOLDER / "old").iterdir():

		print("RSS:", file.name)

		if SUBSET and file.name.split(".")[0] not in SUBSET:
			continue

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
		
		item['_source'] = get_search(item['_source'])
		item['_index'] = "news"
		item['_source']['source'] = "cnbc"
		item['_source']['authors'] = item['_source']['authors'].strip()
		item['_source']['article_type'] = item['_source']['article_type'].strip()

		return item

	for file in (CNBC_FOLDER / "old").iterdir():

		print("CNBC:", file.name)
		if SUBSET and file.name.split("_")[2] not in SUBSET:
			continue

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
		item['_source'] = get_search(item['_source'])
		item['_index'] = "news"
		item['_source']['source'] = "google"
		return item

	for file in (GOOGLE_FOLDER / "old").iterdir():

		print("GOOGLE:", file.name)
		if SUBSET and file.name.split("_")[2] not in SUBSET:
			continue

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