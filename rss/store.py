from datetime import datetime, timedelta
from const import DIR, CONFIG, logger
from hashlib import sha256
import tarfile as tar
import pandas as pd
import sys, os
import json
import time

sys.path.append(f"{DIR}/../utils")
from gcp import send_to_bucket, send_gcp_metric

def compress_files():

	filedate = datetime.now() - timedelta(days = 1)
	filedate = filedate.strftime('%Y-%m-%d')
	
	raw_txt = f'{DIR}/news_data_backup/{filedate}.txt'
	raw_tar = f'{DIR}/news_data_backup/{filedate}.tar.xz'

	files = os.listdir(f"{DIR}/news_data")
	files = [f"{DIR}/news_data/{file}" for file in files]
	files = sorted(files, key=os.path.getmtime)[::-1]
	files.remove(f"{DIR}/news_data/.gitignore")

	cfiles = os.listdir(f"{DIR}/cleaned_news_data")
	cfiles = [f"{DIR}/cleaned_news_data/{file}" for file in cfiles]
	cfiles = sorted(cfiles, key=os.path.getmtime)[::-1]
	cfiles.remove(f"{DIR}/cleaned_news_data/.gitignore")

	###############################################################################################

	ctr = 0
	data, hashes = list(), set()
	sources, usources = dict(), dict()
	for file in files:

		with open(file, "r") as data_file:
			items = json.loads(data_file.read())

		for item in items:

			ctr += 1

			item_ = item.copy()
			item_.pop("oscrap_acquisition_datetime")

			if 'oscrap_source' not in item_:
				continue

			source = item_['oscrap_source']
			if source in sources:
				sources[source] += 1
			else:
				sources[source] = 1

			hash_ = sha256(json.dumps(item_).encode()).hexdigest()

			if hash_ in hashes:
				continue

			if source in usources:
				usources[source] += 1
			else:
				usources[source] = 1

			data.append(item)
			hashes.add(hash_)

	logger.info(f"RSS,Storage,Data,{ctr}")
	logger.info(f"RSS,Storage,Unique Data,{len(hashes)}")
	
	send_gcp_metric(CONFIG, "rss_daily_item_uniques", "int64_value", len(hashes))
	send_gcp_metric(CONFIG, "rss_daily_item_total", "int64_value", ctr)

	for source in sources:
		
		logger.info(f"RSS,Source Total,{source},{sources[source]}")
		metric_name = source.lower().replace(" ", "_")
		send_gcp_metric(CONFIG, f"{metric_name}_daily_item_total", "int64_value", sources[source])

	for source in usources:

		logger.info(f"RSS,Source Uniques,{source},{usources[source]}")
		metric_name = source.lower().replace(" ", "_")
		send_gcp_metric(CONFIG, f"{metric_name}_daily_item_uniques", "int64_value", usources[source])

	with open(raw_txt, "w") as file:
		file.write(json.dumps(data))

	with tar.open(raw_tar, mode="x:xz") as tar_file:
		tar_file.add(raw_txt, arcname=os.path.basename(raw_txt))

	###############################################################################################

	ctr = 0
	data, hashes = list(), set()
	for file in cfiles:

		with open(file, "r") as data_file:
			items = json.loads(data_file.read())

		for item in items:

			ctr += 1

			hash_ = sha256(
				json.dumps(item).encode()
			).hexdigest()

			if hash_ in hashes:
				continue

			data.append(item)
			hashes.add(hash_)

	send_gcp_metric(CONFIG, "rss_daily_clean_uniques", "int64_value", len(hashes))
	send_gcp_metric(CONFIG, "rss_daily_clean_total", "int64_value", ctr)

	cleaned_txt = f"{DIR}/cleaned_news_data/{filedate}.txt"
	cleaned_tar = cleaned_txt[:-4] + ".tar.xz"

	with open(cleaned_txt, "w") as file:
		file.write(json.dumps(data))

	with tar.open(cleaned_tar, mode="x:xz") as tar_file:
		tar_file.add(cleaned_txt, arcname=os.path.basename(cleaned_txt))

	###############################################################################################

	time.sleep(600)

	file_size = os.stat(raw_tar).st_size / 1_000_000
	if file_size > 0:
		for file in files:
			os.remove(file)
		os.remove(raw_txt)
	else:
		raise Exception("TarFile Corrupted. File Size 0.")

	file_size = os.stat(cleaned_tar).st_size / 1_000_000
	if file_size > 0:
		for file in cfiles:
			os.remove(file)
		os.remove(cleaned_txt)
	else:
		raise Exception("TarFile Corrupted. File Size 0.")

	return raw_tar, cleaned_tar

if __name__ == '__main__':

	try:

		raw_tar, cleaned_tar = compress_files()

		send_to_bucket(
			CONFIG['gcp_bucket_prefix'],
			CONFIG['gcp_bucket_name'],
			os.path.basename(raw_tar),
			os.path.dirname(raw_tar),
			logger=logger
		)

		send_to_bucket(
			f"cleaned_{CONFIG['gcp_bucket_prefix']}",
			CONFIG['gcp_bucket_name'],
			os.path.basename(cleaned_tar),
			os.path.dirname(cleaned_tar),
			logger=logger
		)
		os.remove(cleaned_tar)

		logger.info(f"RSS,Storage,Success,")

	except Exception as e:

		logger.warning(f"RSS,Storage,Failure,{e}")
