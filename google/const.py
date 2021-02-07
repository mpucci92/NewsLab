from datetime import datetime
import sqlalchemy as sql
import sys, os
import pytz
import json

DIR = os.path.realpath(os.path.dirname(__file__))
DATE = datetime.now(pytz.timezone("Canada/Eastern"))
SDATE = DATE.strftime("%Y-%m-%d")

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