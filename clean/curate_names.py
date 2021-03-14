from lists import *
import pandas as pd
import inflect

###################################################################################################

stop_words = set(pd.read_csv("data/stop_words.csv").word.str.lower())
eng_words = set(pd.read_csv("data/english_words.csv").word.str.lower())
countries = set(pd.read_csv("data/countries.csv").word.str.lower())
currencies = set(pd.read_csv("data/currencies.csv").word.str.lower())
two_grams = set(pd.read_csv("data/two_grams.csv").gram.str.lower())
commodities = set(pd.read_csv("data/commodities.csv").commodity.str.lower())
inflect_engine = inflect.engine()

###################################################################################################

def pre_filter(df):
	
	combo = df.name + " " + df.exchange
	vcs = combo.value_counts()    
	df = df[combo.isin(vcs[vcs == 1].index)]
	df = df[df.ticker.str.count("\\.") <= 1]

	## Remove warants and units from NASDAQ tickers
	ndaq = df[df.exchange == 'NASDAQ']
	ndaq = ndaq[ndaq.ticker.str.len() > 4]
	df = df[~df.index.isin(ndaq.index)]

	## Remove warrants and units from other tickers
	## Keep class A and B shares only
	ticker_mods = df[df.ticker.str.count("\\.") == 1]
	mod = ticker_mods.ticker.str.split("\\.").str[-1]
	ticker_mods = ticker_mods[~mod.isin(["A", "B"])]
	df = df[~df.index.isin(ticker_mods.index)]

	return df

###################################################################################################

def remove_suffix(df):
	
	## Recursively remove suffixes in company names
	stats = []
	for vals in df.values:

		ticker, name, exch = vals

		stats.append([ticker, name, exch, 'none'])

		tokens = name.split(" ")
		while tokens[-1] in SUFFIXES:
			tokens = tokens[:-1]
			stats.append([ticker, ' '.join(tokens), exch, 'suff'])

	df = pd.DataFrame(stats, columns = ['ticker', 'name', 'exchange', 'type'])
	return df.drop_duplicates()

def replace_special_cases(df):
	
	df['name'] = df.name + " "

	new_names = list(df.name)
	new_names += list(df.name.str.replace("\\.com ", " "))
	new_names += list(df.name.str.replace("'s ", "s "))
	new_names += list(df.name.str.replace(" & ", " and "))
	new_names += list(df.name.str.replace("-", " "))
	new_names += list(df.name.str.replace("-", ""))
	
	n = 7
	tickers = list(df.ticker)*n
	exchanges = list(df.exchange)*n
	types = list(df.type)*n
	
	df = pd.DataFrame(list(zip(new_names, tickers, exchanges, types)))
	df.columns = ['name', 'ticker', 'exchange', 'type']
	df['name'] = df.name.str.strip()

	return df

def remove_single_stop_and_english_words(df):
	
	words = df[df.name.isin(stop_words)]
	words = words[~words.name.isin(SAFE_SUFF_SINGLES)]
	df = df[~df.index.isin(words.index)]

	words = df[df.name.isin(eng_words)]
	words = words[~words.name.isin(SAFE_SUFF_SINGLES)]
	return df[~df.index.isin(words.index)]

###################################################################################################

def remove_modifiers(df):

	stats = []
	for row in df.values:

	    name, ticker, exchange, _type = row

	    stats.append([name, ticker, exchange, _type])
	    
	    tokens = name.split(" ")
	    while tokens[-1] in MODIFIERS and len(tokens) > 1:
	        tokens = tokens[:-1]
	        stats.append([' '.join(tokens), ticker, exchange, 'modifier'])

	df = pd.DataFrame(stats, columns = df.columns)
	return df.drop_duplicates()

def remove_modifier_stop_and_english_words(df):

	mods = df[df.type == 'modifier']
	mods = mods[mods.name.isin(stop_words)]
	df = df[~df.index.isin(mods.index)]

	mods = df[df.type == 'modifier']
	mods = mods[mods.name.str.split(" ").str.len() == 1]
	mods = mods[mods.name.isin(eng_words)]
	mods = mods[~mods.name.isin(SAFE_MOD_SINGLES)]
	return df[~df.index.isin(mods.index)]

def remove_modifier_duplicates(company_names):

	def remove_duplicates(df, length):

		mods = df[df.type == 'modifier']
		mods = mods[mods.name.str.split(" ").str.len() == length]

		vcs = mods.name.value_counts()
		singles = vcs[vcs > 1].index
		banned_singles = []

		for single in singles:
		
		    tickers = mods[mods.name == single].ticker
		    names = df[df.ticker.isin(tickers)]
		    doubles = names[names.name.str.split(" ").str.len() == length + 1]
		
		    if doubles.name.nunique() > 1:
		        banned_singles.append(single)

		return df[~df.name.isin(banned_singles)]

	company_names = remove_duplicates(company_names, 1)
	return remove_duplicates(company_names, 2)

###################################################################################################

def replace_synonyms(df):
	
	df['name'] = df.name.str.strip() + " "
	df1 = df.copy()
	df2 = df.copy()
	
	for syn in SYNONYMS:
		df1['name'] = df1.name.str.replace(*syn)    
	for syn in SYNONYMS:
		df2['name'] = df2.name.str.replace(*syn[::-1])

	df['name'] = df.name.str.strip()
	df1['name'] = df1.name.str.strip()
	df2['name'] = df2.name.str.strip()
	
	return pd.concat([df, df1, df2]).drop_duplicates()

def replace_all_synonyms(df):
	
	while True:
		new_df = replace_synonyms(df)
		print(len(df), len(new_df))
		if len(new_df) == len(df):
			break
		df = new_df.copy()
		
	return df

def remove_short_names(df):

	short_names = df[df.name.str.len() == 2]
	short_names = short_names[~short_names.name.isin(SAFE_SUFF_SINGLES)]
	return df[~df.index.isin(short_names.index)].reset_index(drop=True)

def remove_all_number_names(df):

	return df[~df.name.str.isnumeric()].reset_index(drop=True)

def replace_indices(df):
	
	new = [
		['dow', 'DIA', 'NASDAQ', 'suff'],
		['dow jones industrial average', 'DIA', 'NASDAQ', 'none'],
		['dow jones', 'DIA', 'NASDAQ', 'none'],
		['nasdaq composite', 'QQQ', 'NASDAQ', 'none'],
		['nasdaq', 'QQQ', 'NASDAQ', 'suff']
	]
	
	df = df[df.name != 'dow']
	return pd.concat([
		df,
		pd.DataFrame(new, columns = df.columns)
	]).reset_index(drop=True)

def remove_countries_and_currencies(df):

	df = df[~df.name.isin(countries)]
	df = df[~df.name.isin(currencies)]
	return df

def remove_english_two_grams(df):

	names = df[df.name.isin(two_grams)].name
	to_remove = []

	for name in names:

		if name in SAFE_TWO_GRAMS:
			continue

		w1, w2 = name.split(" ")
		if w2 in SUFFIXES:
			continue

		to_remove.append(name)
		to_remove.append(inflect_engine.plural(name))
		
		sw2 = inflect_engine.singular_noun(name)
		if sw2:
			to_remove.append(sw2)

	return df[~df.name.isin(to_remove)]

def remove_commodities(df):

	return df[~df.name.isin(commodities)]

def add_nicknames(df):

	nicknames = [
		['google', 'GOOG', 'NASDAQ', 'custom'],
		['jpm', 'JPM', 'NYSE', 'custom'],
		['j&j', 'JNJ', 'NYSE', 'custom'],
		['jnj', 'JNJ', 'NYSE', 'custom'],
		['walmart', 'WMT', 'NYSE', 'custom'],
		['wal-mart', 'WMT', 'NYSE', 'custom'],
		['disney', 'DIS', 'NYSE', 'custom'],
		['p&g', 'PG', 'NYSE', 'custom'],
		['exxon', 'XOM', 'NYSE', 'custom'],
		['toyota', 'TM', 'NYSE', 'custom'],
		['tmobile', 'TMUS', 'NASDAQ', 'custom'],
		['citi', 'C', 'NYSE', 'custom'],
		['costco', 'COST', 'NASDAQ', 'custom'],
		['charles schwab', 'SCHW', 'NYSE', 'custom'],
		['sanofi', 'SNY', 'NASDAQ', 'custom'],
		['amex', 'AXP', 'NYSE', 'custom'],
		['ge', 'GE', 'NYSE', 'custom'],
		['ibm', 'IBM', 'NYSE', 'custom'],
		['zoom', 'ZM', 'NASDAQ', 'custom'],
		['amd', 'AMD', 'NASDAQ', 'custom'],
		['advanced micro devices', 'AMD', 'NASDAQ', 'custom'],
		['glaxo smith skline', 'GSK', 'NASDAQ', 'custom'],
		['snapchat', 'SNAP', 'NYSE', 'custom'],
		['colgate', 'CL', 'NYSE', 'custom'],
		['snapchat', 'SNAP', 'NYSE', 'custom'],
		['ford', 'F', 'NYSE', 'custom'],
		['kraft', 'KHC', 'NASDAQ', 'custom'],
		['lululemon', 'LULU', 'NASDAQ', 'custom'],
		['motorola', 'MSI', 'NYSE', 'custom'],
		['merril lynch', 'IPB', 'AMEX', 'custom'],
	]
	df = pd.concat([
		df,
		pd.DataFrame(nicknames, columns = df.columns)
	])
	return df

def curate_names(company_names):

	company_names['name'] = company_names.name.str.lower()
	print("Initial", company_names.shape)
	company_names = pre_filter(company_names)

	## Suffix Section
	print("Pre Filter", company_names.shape)
	company_names = remove_suffix(company_names)
	print("Remove Suffix", company_names.shape)
	company_names = replace_special_cases(company_names)
	print("Special Cases", company_names.shape)
	company_names = remove_single_stop_and_english_words(company_names)

	## Modifier Section
	print("Single stop and english", company_names.shape)
	company_names = remove_modifiers(company_names)
	print("Modifiers", company_names.shape)
	company_names = remove_modifier_stop_and_english_words(company_names)
	print("Modifier stop and english", company_names.shape)
	company_names = remove_modifier_duplicates(company_names)
	print("Modifier Duplicates", company_names.shape)

	## Final cleaning
	exchanges = ["AMEX", "NASDAQ", "NYSE", "TSX", "LSE"]
	company_names = company_names[company_names.exchange.isin(exchanges)]
	print("Exchange Filter", company_names.shape)
	company_names = replace_all_synonyms(company_names)
	print("Replace Synonyms", company_names.shape)
	company_names = replace_indices(company_names)
	print("Replace Indices", company_names.shape)
	company_names = remove_all_number_names(company_names)
	print("Remove Numbered Names", company_names.shape)
	company_names = remove_short_names(company_names)
	print("Remove Short Names", company_names.shape)
	company_names = company_names[~company_names.name.isin(MANUAL_OVERRIDES)]
	print("Remove Manual Overrides", company_names.shape)
	company_names = remove_countries_and_currencies(company_names)
	print("Remove Countries and Currencies", company_names.shape)
	company_names = remove_english_two_grams(company_names)
	print("Remove English Two Grams", company_names.shape)
	company_names = remove_commodities(company_names)
	print("Remove Commodities", company_names.shape)

	## Super special case of 'Target'. Add nicknames
	company_names = company_names[company_names.name != 'target']
	print("Remove Target", company_names.shape)
	company_names = add_nicknames(company_names)
	print("Added Nicknames", company_names.shape)

	## Sort and Save
	company_names = company_names.sort_values('ticker')
	company_names = company_names.drop_duplicates().reset_index(drop=True)
	print("Drop dupes", company_names.shape)
	return company_names

###################################################################################################
	
if __name__ == '__main__':

	company_names = pd.read_csv("data/company_names.csv")
	company_names = curate_names(company_names)
	company_names.to_csv("data/cleaned_company_names.csv", index=False)
