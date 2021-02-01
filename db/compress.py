from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, SUBSET
import tarfile as tar
import sys, os
import json

def compress():

	for folder, name in zip([RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER], ["RSS", "CNBC", "GOOGLE"]):
		
		print("\n\n\n___***___***___***___***___***___***___")
		print("\nProcessing", name, "\n")
		print("___***___***___***___***___***___***___\n\n")

		for file in (folder / "new").iterdir():

			print(f"{name}:", file.name)
			if "_" in file.name:
				sep, idx = "_", 2
			else:
				sep, idx = ".", 0

			if SUBSET and file.name.split(sep)[idx] not in SUBSET:
				continue

			tar_file = folder / "tar" / file.with_suffix(".tar.xz").name
			with tar.open(tar_file, "x:xz") as _tar_file:
				_tar_file.add(file, arcname=file.name)

if __name__ == '__main__':

	compress()
