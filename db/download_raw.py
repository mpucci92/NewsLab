from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, RAWDIR, UZDIR, ZDIR
from filetype import guess
import tarfile as tar

def init_dirs(DIR):

	if not DIR.is_dir():
		DIR.mkdir()

	_dir = (DIR / "cnbc")
	if not _dir.is_dir():
		_dir.mkdir()

	_dir = (DIR / "google")
	if not _dir.is_dir():
		_dir.mkdir()

	_dir = (DIR / "rss")
	if not _dir.is_dir():
		_dir.mkdir()

if __name__ == '__main__':

	init_dirs(RAWDIR)
	init_dirs(UZDIR)
	init_dirs(ZDIR)

	# ## RSS
	# for blob in RSS_BUCKET.list_blobs():

	# 	if 'rss' not in blob.name or 'cleaned' in blob.name:
	# 		continue

	# 	parent, name = blob.name.split("/")
	# 	file = RAWDIR / "rss" / name

	# 	if not file.exists():

	# 		print("Downloading rss:", blob.name)
	# 		blob.download_to_filename(file)

	# 		with tar.open(file, "r:xz") as tar_file:
	# 			tar_file.extractall(UZDIR / "rss")

	# for blob in BUCKET.list_blobs():

	# 	if 'twitter' in blob.name.lower():
	# 		continue

	# 	parent, name = blob.name.split("/")
	# 	if not name:
	# 		continue

	# 	# if parent == "CNBCNews":

	# 	# 	file = RAWDIR / "cnbc" / name

	# 	# 	if not file.exists():

	# 	# 		print("Downloading CNBC:", name)
	# 	# 		blob.download_to_filename(file)

	# 	# 		ftype = guess(str(file))
	# 	# 		if ftype:
	# 	# 			with tar.open(file, "r:gz") as tar_file:
	# 	# 				tar_file.extractall(UZDIR / "cnbc")
	# 	# 		else:
	# 	# 			print("FAULT ON", file)

	# 	if parent == "GoogleNews":

	# 		file = RAWDIR / "google" / name
	# 		if not file.exists():

	# 			print("Downloading Google:", name)
	# 			blob.download_to_filename(file)

	# 			ftype = guess(str(file))
	# 			if ftype:
	# 				print(ftype)
	# 				with tar.open(file, "r:gz") as tar_file:
	# 					tar_file.extractall(UZDIR / "google")
	# 			else:
	# 				print("FAULT ON", file)
