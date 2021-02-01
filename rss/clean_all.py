from elasticsearch import Elasticsearch, helpers
from const import DIR, CONFIG, ES_MAPPINGS
from clean import clean, get_scores
from google.cloud import storage
from hashlib import sha256
from pathlib import Path
import tarfile as tar
import sys, os
import json

###################################################################################################

BUCKET = storage.Client().bucket(CONFIG["gcp_bucket_name"])
PATH = Path(f"{DIR}/tmp/rss_data")
CPATH = Path(f"{DIR}/tmp/cleaned_rss_data")
TPATH = Path(f"{DIR}/tmp/tar_rss_data")

SUBSET = [
]

###################################################################################################

def get_date(filename):
	return filename.split(".")[0]

def download():

	print("DOWNLOAD")

	if not PATH.is_dir():
		PATH.mkdir()

	for i, blob in enumerate(BUCKET.list_blobs()):

		if 'rss/' not in blob.name:
			continue

		name = blob.name[4:]
		print(name)

		if SUBSET:
			if get_date(name) not in SUBSET:
				continue

		blob.download_to_filename(PATH / name)
		with tar.open(PATH / name, "r:xz") as tar_file:
			tar_file.extractall(path=PATH)
		os.remove(PATH / name)

def clean_items():

	print("CLEAN ITEMS")

	if not CPATH.is_dir():
		CPATH.mkdir()

	for i, file in enumerate(sorted(PATH.iterdir())):

		print(file.name)

		if SUBSET:
			if get_date(file.name) not in SUBSET:
				continue

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		items = [
			clean(item)
			for item in items
			if item.get('title')
		]

		for item in items:

			item['search'] = [item.get("title")]
			summary = item.get("summary", "")

			if summary:
				item['search'].append(summary)

		with open(CPATH / file.name, "w") as _file:
			_file.write(json.dumps(items))

def remove_duplicates():

	print("REMOVE DUPLICATES")

	hashs = set()

	for i, file in enumerate(sorted(CPATH.iterdir())):

		print(file.name)

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		uniques = []
		for item in items:

			_hash = sha256(json.dumps({
				'title' : item['title'].lower(),
				'summary' : item['summary'].lower(),
				'link' : item['link'].lower()
			}).encode()).hexdigest()

			if _hash in hashs:
				continue

			uniques.append(item)
			hashs.add(_hash)

		if SUBSET:
			if get_date(file.name) not in SUBSET:
				continue

		with open(file, "w") as _file:
			_file.write(json.dumps(uniques))

def get_sentiment():

	print("CALCULATE SENTIMENT")

	for i, file in enumerate(sorted(CPATH.iterdir())):

		print(file.name)

		if SUBSET:
			if get_date(file.name) not in SUBSET:
				continue

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		titles = [
			item['title']
			for item in items
		]
		scores = get_scores(titles)

		print(len(titles), len(scores))

		for item, score in zip(items, scores):
			item['sentiment'] = score['prediction']
			item['sentiment_score'] = score['sentiment_score']
			item['abs_sentiment_score'] = abs(score['sentiment_score'])

		with open(CPATH / file.name, "w") as _file:
			_file.write(json.dumps(items))

def index():

	print("INDEX")

	es = Elasticsearch([CONFIG['ES_IP']], timeout=60_000)
	# try:
	# 	es.indices.delete(index="rss")
	# except Exception as e:
	# 	print(e)
	# es.indices.create(index='rss',body=ES_MAPPINGS)

	items = []
	for i, file in enumerate(sorted(CPATH.iterdir())):

		print(file.name)

		if SUBSET:
			if get_date(file.name) not in SUBSET:
				continue

		with open(file, "r") as _file:
			new_items = json.loads(_file.read())

		hashs = [
			sha256(json.dumps({
				'title' : item['title'],
				'summary' : item['summary'],
				'link' : item['link']
			}).encode()).hexdigest()
			for item in new_items
		]

		new_items = [
			{
				"_index" : "news",
				"_op_type" : "create",
				"_id" : _hash,
				"_source" : item
			}
			for _hash, item in zip(hashs, new_items)
		]
		items.extend(new_items)

		with open(file, "w") as _file:
			_file.write(json.dumps(new_items))

	# successes, failures = helpers.bulk(es, items, stats_only=True, raise_on_error=False, chunk_size=25_000, max_retries=2)
	# print(successes, failures)

def tar_it():

	if not TPATH.is_dir():
		TPATH.mkdir()

	for file in sorted(CPATH.iterdir()):

		print(file.name)
		with tar.open(TPATH / file.with_suffix(".tar.xz").name, "x:xz") as tar_file:
			tar_file.add(file, arcname=file.name)

if __name__ == '__main__':

	# download()
	# clean_items()
	# remove_duplicates()
	# get_sentiment()
	# index()
	tar_it()
	# pass
