from datetime import datetime
import sqlalchemy as sql
import pandas as pd
import logging
import sys, os
import pytz
import json

###################################################################################################

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

###################################################################################################

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

        logger.warning(f"hash cache error. {key}. {e}")
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

def save(key, path, hash_cache, send_to_bucket):

    items = []
    for file in path.iterdir():
        
        if file.name == ".gitignore":
            continue

        with open(file, "r") as _file:
            items.extend(json.loads(_file.read()))

    json_file = Path(f"{DIR}/news_data/{key}/{SDATE}.json")
    xz_file = json_file.with_suffix(".tar.xz")

    with open(f"{DIR}/data/{key}_hash_cache.json", "w") as file:
        file.write(json.dumps(hash_cache))

    with open(json_file, "w") as file:
        file.write(json.dumps(items))

    with tar.open(xz_file, "x:xz") as tar_file:
        tar_file.add(json_file, arcname=json_file.name)

    send_to_bucket(key,
                   CONFIG['GCP']['RAW_BUCKET'],
                   xz_file.name,
                   xz_file.parent,
                   logger)

    logger.info(f"{key} job, sleeping")
    time.sleep(10)

    for file in path.iterdir():
        if file.name == ".gitignore":
            continue
        file.unlink()

    os.unlink(json_file)
    os.unlink(xz_file)