from google.cloud import storage
from pathlib import Path
import json

RSS_BUCKET = storage.Client().bucket("oscrap_storage")
RSS_FOLDER = Path("data/rss/")

BUCKET = storage.Client().bucket("cnbc-storage")
CNBC_FOLDER = Path("data/cnbc/")
GOOGLE_FOLDER = Path("data/google/")

with open("config.json", "r") as file:
	CONFIG = json.loads(file.read())