from google.cloud import storage
from pathlib import Path
import json
import os

DIR = os.path.realpath(os.path.dirname(__file__))

RSS_BUCKET = storage.Client().bucket("oscrap_storage")
RSS_FOLDER = Path("news_data/rss/")

BUCKET = storage.Client().bucket("cnbc-storage")
CNBC_FOLDER = Path("news_data/cnbc/")
GOOGLE_FOLDER = Path("news_data/google/")

SUBSET = []

with open(f"{DIR}/../config.json", "r") as file:
	CONFIG = json.loads(file.read())
