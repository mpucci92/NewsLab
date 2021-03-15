from download_company_names import download_company_names
from curate_company_names import curate_company_names
from const import CONFIG, DIR, logger
from pathlib import Path
import pandas as pd
import traceback
import sys, os
import time

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_email

def get_diff(df1, df2):
	return df1[~df1.apply(tuple,1).isin(df2.apply(tuple,1))]

if __name__ == '__main__':

	logger.info("company name downloader & curator initialized")
	metric = "company_names_success_indicator"

	try:

		ocomnames = pd.read_csv(f"{DIR}/data/company_names.csv")
		ocurnames = pd.read_csv(f"{DIR}/data/curated_company_names.csv")

		comnames = download_company_names()
		curnames = curate_company_names(comnames)

		new_comnames = get_diff(comnames, ocomnames)
		removed_comnames = get_diff(ocomnames, comnames)

		new_curnames = get_diff(curnames, ocurnames)
		removed_curnames = get_diff(ocurnames, curnames)

		comnames.to_csv(f"{DIR}/data/company_names.csv", index=False)
		curnames.to_csv(f"{DIR}/data/curated_company_names.csv", index=False)

		body = '\n'.join([
			"New Company Names",
			new_comnames.to_html(index=False),
			"\nRemoved Company Names",
			removed_comnames.to_html(index=False),
			"\nNew Curated Names",
			new_curnames.to_html(index=False),
			"\nRemoved Curated Names",
			removed_curnames.to_html(index=False),
		])

		n = new_comnames.shape[0] + removed_comnames.shape[0]
		n += new_curnames.shape[0] + removed_curnames.shape[0]
		if n > 0:
			send_email(CONFIG, "Company Name Summary", body, [], logger)

		logger.info("company name downloader & curator succesful")
		send_metric(CONFIG, metric, "int64_value", 1)

	except Exception as e:

		exc = traceback.format_exc()
		logger.warning(f"company name downloader & curator failed, {e}, {exc}")
		send_metric(CONFIG, metric, "int64_value", 0)

	logger.info("company name downloader & curator terminated")