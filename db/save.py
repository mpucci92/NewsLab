from const import CONFIG, DIR, SDATE, logger
from pathlib import Path
import tarfile as tar
import sys, os
import json

sys.path.append(f"{DIR}/..")
from utils import send_to_bucket

def save():

	folder = Path(f"{DIR}/cleaned_data")
	files = list(folder.iterdir())
	files.remove(folder / ".gitignore")
	
	items = []
	for file in files:
		with open(file, "r") as file:
			items.extend(json.loads(file.read()))

	json_file = folder / f"{SDATE}.json"
	xz_file = json_file.with_suffix(".tar.xz")

	counts = {
		"google" : 0,
		"cnbc" : 0,
		"rss" : 0
	}
	for item in items:
		counts[item['source']] += 1

	with open(json_file, "w") as file:
		file.write(json.dumps(items))

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(json_file, arcname=json_file.name)

	send_to_bucket(CONFIG,
				   CONFIG['GCP']['CLEAN_BUCKET'],
				   xz_file.name,
				   xz_file.parent,
				   logger)

	json_file.unlink()
	tar_file.unlink()

	for file in files:
		file.unlink()