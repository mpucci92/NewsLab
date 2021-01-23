from datetime import datetime
import logging
import socket
import json
import os

###################################################################################################

date_today = datetime.today().strftime("%Y-%m-%d")

DIR = os.path.realpath(os.path.dirname(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/rss.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################

with open(f"{DIR}/../config.json", "r") as file:
	CONFIG = json.loads(file.read())

date = datetime.today().strftime("%Y-%m-%d")
with open(f"{DIR}/../config.json", "w") as file:
	
	CONFIG['date'] = date

	if socket.gethostname() == CONFIG['gcp_hostname']:
		CONFIG['db'] = "compour9_finance"
		CONFIG['gcp_bucket_prefix'] = "rss"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	file.write(json.dumps(CONFIG))

###################################################################################################

ES_MAPPINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis" : {
            "analyzer" : {
                "search_analyzer" : {
                    "tokenizer" : "classic",
                    "filter" : [
                        "classic",
                        "lowercase",
                        "stop",
                        "trim",
                        "length_limiter",
                        "shingler"
                    ]
                }
            },
            "filter" : {
                "length_limiter" : {
                    "type" : "length",
                    "min" : 2
                },
                "shingler" : {
                    "type" : "shingle",
                    "min_shingle_size" : 2,
                    "max_shingle_size" : 3
                },
            }
        }
    },
    "mappings": {
        "properties": {
                "title": {
                    "type" : "text",
                    "analyzer" : "search_analyzer"
                },
                "summary" : {
                    "type" : "text",
                    "analyzer" : "search_analyzer"
                },
                "_summary" : {
                    "type" : "text"
                },
                "tables" : {
                	"type" : "text"
                },
                "link" : {
                    "type" : "keyword"
                },
                "timestamp" : {
                    "type" : "date",
                },
                "oscrap_timestamp": {
                    "type" : "date",
                },
                "authors" : {
                    "type" : "keyword"
                },
                "article_type" : {
                    "type" : "keyword"
                },
                "article_source" : {
                	"type" : "keyword"
                },
                "source" : {
                	"type" : "keyword"
                },
                "tickers" : {
                    "type" : "keyword"
                },
                "language" : {
                    "type" : "keyword"
                },
                "categories" : {
                    "type" : "keyword"
                },
                "related" : {
                    "type" : "keyword"
                },
                "_tickers" : {
                    "type" : "keyword"
                },
                "credit": {
                    "type" : "keyword"
                },
                "sentiment": {
                	"type" : "keyword"
                },
                "sentiment_score" : {
                	"type" : "float"
                }
            }
        }
    }