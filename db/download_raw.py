from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, SUBSET, RAWDIR

def init_dirs():

	if not RAWDIR.is_dir():
		RAWDIR.mkdir()

	_dir = (RAWDIR / "cnbc")
	if not _dir.is_dir():
		_dir.mkdir()

	_dir = (RAWDIR / "google")
	if not _dir.is_dir():
		_dir.mkdir()

	_dir = (RAWDIR / "rss")
	if not _dir.is_dir():
		_dir.mkdir()

if __name__ == '__main__':

	init_dirs()

	for i, blob in enumerate(BUCKET.list_blobs()):

		if 'twitter' in blob.name.lower():
			continue

		parent, name = blob.name.split("/")
		if not name:
			continue

		if parent == "CNBCNews":

			print("Downloading CNBC:", name)
			file = RAWDIR / "cnbc" / name
			blob.download_to_filename(file)

		elif parent == "GoogleNews":

			print("Downloading Google:", name)
			file = RAWDIR / "google" / name
			blob.download_to_filename(file)