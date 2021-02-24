from const import DIR, CONFIG, ENGINE, SDATE, DATE, logger
from datetime import datetime, timedelta
from pathlib import Path
from hashlib import md5
import tarfile as tar
import pandas as pd
import json
import time

FMT = "%Y-%m-%d"

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

    TDAY = datetime(DATE.year, DATE.month, DATE.day)
    file = Path(f"{DIR}/data/{key}_hash_cache.json")
    
    if file.exists():

        with open(file, "r") as _file:
            hash_cache = json.loads(_file.read())

        for date in hash_cache.keys():

            dt = datetime.strptime(date, FMT)

            if (TDAY - dt).days > 7:
            
                del hash_cache[date]
                hash_cache[SDATE] = []

    else:

        logger.warning(f"{key} hash cache does not exist")
        hash_cache = {
            (TDAY - timedelta(days=i)).strftime(FMT) : []
            for i in range(7)
        }

    hashs = set([
        _hash
        for hash_list in hash_cache.values()
        for _hash in hash_list
    ])

    return hash_cache, hashs

def save(key, path, hash_cache, hashs, send_to_bucket, send_metric):

    files = list(path.iterdir())
    files.remove(path / ".gitignore")

    items = []
    for file in files:
        
        with open(file, "r") as _file:

            for item in json.loads(_file.read()):

                dummy_item = item.copy()
                dummy_item.pop('acquisition_datetime')

                _hash = md5(json.dumps(dummy_item).encode()).hexdigest()
                if _hash in hashs:
                    continue

                hash_cache[SDATE].append(_hash)
                hashs.add(_hash)
                items.append(item)

    ###############################################################################################

    json_file = path.parent.parent / 'news_data_backup'
    json_file = json_file / f'{key}/{SDATE}.json'
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

    send_metric(CONFIG, f"{key}_raw_news_count", "int64_value", len(items))

    logger.info(f"{key} job, sleeping")
    time.sleep(1)

    json_file.unlink()
    xz_file.unlink()

    for file in files:
        file.unlink()