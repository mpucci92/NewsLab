import pandas as pd
import inflect

eng_words = pd.read_csv("data/english_words.csv")
stop_words = pd.read_csv("data/stop_words.csv")
inflect_engine = inflect.engine()

if __name__ == '__main__':

	words = []
	for word in eng_words.word:
		words.append(word)
		pword = inflect_engine.plural(word)
		if len(pword) > len(word):
			words.append(pword)
	eng_words = pd.DataFrame(words, columns = ['word'])
	eng_words = eng_words.drop_duplicates()
	eng_words.to_csv("data/english_words.csv", index=False)

	words = []
	for word in stop_words.word:
		words.append(word)
		words.append(inflect_engine.plural(word))
	stop_words = pd.DataFrame(words, columns = ['word'])
	stop_words = stop_words.drop_duplicates()
	stop_words.to_csv("data/stop_words.csv", index=False)
