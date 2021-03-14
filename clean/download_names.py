from selenium.webdriver.support.ui import Select
from selenium import webdriver
import chromedriver_binary
from pathlib import Path
from const import DIR
import pandas as pd
import time
import os

###################################################################################################

EXCHANGES = [
	"AMEX",
	"LSE",
	"NASDAQ",
	"NYSE",
	"TSX"
]

DATA = Path(f"{DIR}/name_data")

###################################################################################################

def download_and_merge_names():

	for file in DATA.iterdir():
		file.unlink()

	###############################################################################################

	options = webdriver.ChromeOptions()
	options.add_argument("--headless")
	options.add_argument("--disable-gpu")
	options.add_argument("window-size=1024,768")
	options.add_argument("--no-sandbox")
	options.add_experimental_option("prefs", {
		"download.default_directory" : str(DATA)
	})

	driver = webdriver.Chrome(options = options)

	print("Getting web page...")
	driver.get("http://eoddata.com")

	username_input = driver.find_element_by_id("ctl00_cph1_lg1_txtEmail")
	password_input = driver.find_element_by_id("ctl00_cph1_lg1_txtPassword")
	login_button = driver.find_element_by_id("ctl00_cph1_lg1_btnLogin")

	username_input.send_keys("zQuantz")
	password_input.send_keys("Street101!")
	
	print("Logging in...")
	login_button.click()

	print("Getting download page...")
	driver.get("http://eoddata.com/symbols.aspx")

	for exchange in EXCHANGES:

		print("Downloading", exchange)

		exchange_selector = Select(driver.find_element_by_id("ctl00_cph1_cboExchange"))
		exchange_selector.select_by_value(exchange)
		
		time.sleep(5)

		download_button = driver.find_element_by_id("ctl00_cph1_ch1_divLink")
		download_button = download_button.find_element_by_tag_name("a")
		download_button.click()

		time.sleep(5)

	###############################################################################################

	df = []
	for file in DATA.iterdir():
		df.append(pd.read_csv(file, delimiter="\t"))
		df[-1]['exchange'] = file.name[:-4]

	df = pd.concat(df)
	df.columns = ['ticker', 'name', 'exchange']
	return df.sort_values('ticker').reset_index(drop=True)

if __name__ == '__main__':

	df = download_and_merge_names()
	df.to_csv(f"{DIR}/data/company_names.csv", index=False)
