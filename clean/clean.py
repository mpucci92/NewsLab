from elasticsearch.helpers.errors import BulkIndexError
from elasticsearch import Elasticsearch, helpers
from clean_item import clean_item
from const import DIR, CONFIG
from hashlib import sha256
from pathlib import Path
import requests
import shutil
import sys, os
import json
import time
import uuid

sys.path.append(f"{DIR}/..")
from utils import send_metric

###################################################################################################

# ES_CLIENT = Elasticsearch(CONFIG['ES']['IP'], port=CONFIG['ES']['PORT'], http_comprress=True, timeout=30)
ES_CLIENT = Elasticsearch()
HEADERS = {"Content-Type" : "application/json"}

NEWS_DIRS = [
	Path(f"{DIR}/../rss/news_data"),
	Path(f"{DIR}/../news/news_data/google"),
	# Path(f"{DIR}/../news/news_data/cnbc"),
]

NEWS_DIR = Path(f"{DIR}/news_data")

###################################################################################################

def get_files():
	
	return [
		shutil.copy(file, NEWS_DIR / file.name)
		for _dir in NEWS_DIRS
		for file in list(_dir.iterdir())
	]

def get_scores(sentences):

	data = {"sentences" : sentences}
	response = requests.post("http://localhost:9602", headers=HEADERS, json=data)
	response = json.loads(response.content)
	return response.values()

def cleaning_loop():

	files = set([
		NEWS_DIR / ".gitignore"
	])

	while True:

		new_files = get_files()

		try:
			
			send_metric(
				CONFIG,
				"rss_daily_item_counter",
				"int64_value",
				len(new_files) - 1
			)

		except Exception as e:

			print(e)
				
		items = []
		for new_file in set(new_files).difference(files):
			print(new_file)
			with open(new_file, "r") as file:
				try:
					items.extend(json.loads(file.read()))
					files.add(new_file)
				except Exception as e:
					print(new_file, e)

		new_items = []
		for item in items:

			if not item.get("title"):
				continue

			try:
				item = clean_item(item)
			except Exception as e:
				print(e)
				raise Exception()

			dummy_item = {
				"title" : item['title'].lower(),
				"link" : item['link'].lower()
			}
			
			summary = item.get('summary')
			if summary:
				dummy_item['summary'] = summary

			dummy_item = json.dumps(dummy_item, sort_keys = True)
			_hash = sha256(dummy_item.encode()).hexdigest()

			new_items.append({
				"_index" : "news",
				"_id" : _hash,
				"_op_type" : "create",
				"_source" : item
			})

		if len(new_items) != 0:

			titles = [
				item['_source']['title']
				for item in new_items
			]
			scores = get_scores(titles)

			for item, score in zip(new_items, scores):
				item['_source']['sentiment'] = score['prediction']
				item['_source']['sentiment_score'] = score['sentiment_score']
				item['_source']['abs_sentiment_score'] = abs(score['sentiment_score'])	

			successes, failures = helpers.bulk(ES_CLIENT,
											   new_items,
											   stats_only=True,
											   raise_on_error=False)
			
			print(successes, failures)
			with open(f"{DIR}/cleaned_data/{str(uuid.uuid4())}.json", "w") as file:
				file.write(json.dumps(new_items))

			new_items = []

		time.sleep(5)

if __name__ == '__main__':

	cleaning_loop()
