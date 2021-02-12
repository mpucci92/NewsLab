from datetime import datetime
import sqlalchemy as sql
import pandas as pd
import logging
import sys, os
import pytz
import json

DIR = os.path.realpath(os.path.dirname(__file__))
DATE = datetime.now(pytz.timezone("Canada/Eastern"))
SDATE = DATE.strftime("%Y-%m-%d")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/log.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

with open(f"{DIR}/../news_config.json", "r") as file:
	CONFIG = json.loads(file.read())

ENGINE = sql.create_engine(
    sql.engine.url.URL(
        drivername="mysql",
        username=CONFIG['SQLDB']['USER'],
        password=CONFIG['SQLDB']['PASS'],
        host=CONFIG['SQLDB']['IP'],
        port=CONFIG['SQLDB']['PORT'],
        database='compour9_finance'
    ),
    pool_size=3,
	max_overflow=0,
	pool_recycle=299,
	pool_pre_ping=True
)

file = f"{DIR}/data/ticker_coordinates.csv"
if not os.path.isfile(file):
    df = pd.DataFrame(columns = ['ticker', 'name'])
    df.to_csv(file, index=False)

########################################################################################3##########

def get_ticker_coordinates():

    file = f"{DIR}/data/ticker_coordinates.csv"

    try:

        df = pd.read_sql("""
            SELECT
                ticker,
                name
            FROM
                instruments
            ORDER BY
                market_cap
                DESC
        """, ENGINE)
        
        old_df = pd.read_csv(file)
        df = pd.concat([old_df, df]).drop_duplicates()

        df.to_csv(file, index=False)

    except Exception as e:

        logger.warning(f"ticker coordinate error, {e}")
        df = pd.read_csv(file)

    return df

def get_hash_cache(key):

    try:

        with open(f"{DIR}/data/{key}_hash_cache.json", "r") as file:
            hash_cache = json.loads(file.read())

        hash_cache = {
            int(key) + 1 : hash_cache[key]
            for key in hash_cache
        }
        del hash_cache[7]
        hash_cache[0] = []

    except Exception as e:

        print(e)
        hash_cache = {
            i : []
            for i in range(7)
        }

    hashs = set([
        _hash
        for hash_list in hash_cache.values()
        for _hash in hash_list
    ])

    return hash_cache, hashs