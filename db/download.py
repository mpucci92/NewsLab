from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, SUBSET

import tarfile as tar

def init_folders():

	for parent in [RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER]:

		if not parent.is_dir():
			parent.mkdir()

		for folder in ["old", "new", "tar"]:

			path = parent / folder
			if not path.is_dir():
				path.mkdir()

def download_rss():

	for blob in RSS_BUCKET.list_blobs():

		if "cleaned_rss/" not in blob.name:
			continue

		print("RSS:", blob.name)
		name = blob.name.split("/")[1]
		if SUBSET and name.split(".")[0] not in SUBSET:
			continue

		file = RSS_FOLDER / "old" / name
		blob.download_to_filename(file)
		with tar.open(file, "r:xz") as tar_file:
			tar_file.extractall(path=file.parent)

		file.unlink()

def download():

	for source, folder in zip(["CNBC", "GOOGLE"], [CNBC_FOLDER, GOOGLE_FOLDER]):

		for blob in BUCKET.list_blobs():

			if f"Sentiment {source}" not in blob.name:
				continue

			print(f"{source}:", blob.name)
			name = blob.name.split("/")[1]
			if name == "":
				continue

			if SUBSET and name.split("_")[2] not in SUBSET:
				continue

			file = folder / "old" / name
			blob.download_to_filename(file)

if __name__ == '__main__':

	if not SUBSET:
		init_folders()

	download_rss()
	download()