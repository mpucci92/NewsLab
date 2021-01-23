from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER, CONFIG

from elasticsearch import Elasticsearch, helpers
import json

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
						"porter_stem",
						"shingler",
						"unique"
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
		},
		"similarity" : {
           	"no_length_norm" : {
               	"type" : "BM25",
               	"b" : 0
           	}
        }
	},
	"mappings": {
		"properties": {
				"search" : {
					"type" : "text",
					"analyzer" : "search_analyzer",
					"similarity" : "no_length_norm"
				},
				"search_query" : {
					"type" : "keyword"
				},
				"source_href" : {
					"type" : "keyword"
				},
				"title": {
					"type" : "text"
				},
				"summary" : {
					"type" : "text"
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
				},
				"abs_sentiment_score" : {
					"type" : "float"
				}
			}
		}
	}

###################################################################################################

def terms_filter(field, value):

    return {"terms" : {field : value}}

def range_filter(field, gte=None, lte=None):

    _filter = {}

    if lte:
        _filter.update({"lte" : lte})

    if gte:
        _filter.update({"gte" : gte})

    return {"range" : {field : _filter}}

def search_news(search_string="", sentiment=None, tickers=None, article_source=None, timestamp_from=None,
                timestamp_to=None, sentiment_greater=None, sentiment_lesser=None, language=None, authors=None,
                categories=None):
    
    query = {
        "query" : {
            "function_score" : {
                "query" : {
                    "bool" : {}
                },
                "field_value_factor" : {
                    "field" : "timestamp",
                    "missing" : 0,
                    "factor" : 1
                }
            }                
        }
    }
    
    if search_string:
        query['query']['function_score']['query']['bool']['must'] = [
            {"match" : {"search" : search_string}}
        ]

    filters = []
    if sentiment:
        filters.append(terms_filter("sentiment", sentiment))

    if tickers:
        filters.append(terms_filter("tickers", tickers))

    if language:
        filters.append(terms_filter("language", language))

    if authors:
        filters.append(terms_filter("authors", tickers))

    if categories:
        filters.append(terms_filter("categories", categories))

    if article_source:
        filters.append(terms_filter("article_source", article_source))

    if timestamp_from or timestamp_to:
        filters.append(range_filter("timestamp", timestamp_from, timestamp_to))
        
    if sentiment_greater or sentiment_lesser:
        filters.append(range_filter("sentiment_score", sentiment_greater, sentiment_lesser))
    
    query['query']['function_score']['query']['bool']['filter'] = filters

    return query

###################################################################################################

def index():

	# es = Elasticsearch([f"{CONFIG['ES_IP']}:{CONFIG['ES_PORT']}"])
	es = Elasticsearch()

	try:
		es.indices.delete("news")
	except Exception as e:
		print(e)

	es.indices.create("news", ES_MAPPINGS)

	files = list((RSS_FOLDER / "new").iterdir())
	files += list((CNBC_FOLDER / "new").iterdir())
	files += list((GOOGLE_FOLDER / "new").iterdir())

	items = []
	total_indexed, total_failed = 0, 0
	for i, file in enumerate(files):

		print("Processing:", file.name)

		with open(file, "r") as _file:
			items.extend(json.loads(_file.read()))

		if i > 0 and i % 20 == 0:

			print("Indexing", len(items))

			indexed, failed = helpers.bulk(es,
										   items,
										   stats_only=True,
										   raise_on_error=False)

			print("Indexed:", indexed)
			print("Failed:", failed)

			total_indexed += indexed
			total_failed += failed

			items = []

	indexed, failed = helpers.bulk(es,
								   items,
									stats_only=True,
									raise_on_error=False)

	print("Total Indexed:", total_indexed + indexed)
	print("Total Failed:", total_failed + failed)

if __name__ == '__main__':

	index()
