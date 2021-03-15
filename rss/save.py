from datetime import datetime, timedelta
from const import DIR, CONFIG, logger
from traceback import format_exc
from socket import gethostname
from pathlib import Path
import sys, os

sys.path.append(f"{DIR}/..")
from utils import send_to_bucket, send_metric, save_items

if __name__ == '__main__':

	if gethostname() != CONFIG['MACHINE']['HOSTNAME']:
		CONFIG['GCP']['RAW_BUCKET'] = "tmp_items"

	try:

		filedate = datetime.now() - timedelta(days = 1)
		filedate = filedate.strftime('%Y-%m-%d')

		path = Path(f"{DIR}/news_data")
		xz_file = Path(f"{DIR}/news_data_backup/{filedate}.tar.xz")

		n_items = len(list(path.iterdir()))
		send_metric(CONFIG, "rss_count", "int64_value", n_items - 1)

		n_unique = save_items(path, set(), filedate)
		send_metric(CONFIG, "unique_rss_count", "int64_value", n_items)

		send_to_bucket(
			CONFIG['GCP']['RAW_BUCKET'],
			'rss',
			xz_file,
			logger=logger
		)

		send_to_bucket(
			CONFIG['GCP']['RAW_VAULT'],
			'rss',
			xz_file,
			logger=logger
		)

		logger.info(f"RSS save successeful.")
		send_metric(CONFIG, "rss_save_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"RSS save failed. {e}, {format_exc()}")
		send_metric(CONFIG, "rss_save_success_indicator", "int64_value", 0)
