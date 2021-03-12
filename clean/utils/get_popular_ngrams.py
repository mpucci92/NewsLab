from nltk.corpus import gutenberg, brown, reuters
from string import punctuation
import pandas as pd
import numpy as np
import nltk

def ngrams(tokens, n):
    return zip(*[tokens[i:] for i in range(n+1)])

def corpus_ngrams(sents, n, m):
    grams = [
        [
            ' '.join(gram).lower()
            for gram in ngrams(sent, n - 1)
        ]
        for sent in sents
    ]
    grams = [
        gram
        for arr in grams
        for gram in arr
    ]

    grams, counts = np.unique(grams, return_counts=True)
    grams, counts = grams[counts > m], counts[counts > m]
    idc = np.argsort(counts)[::-1]
    grams, counts = grams[idc], counts[idc]

    gramd = {
        gram : count
        for gram, count in zip(grams, counts)
    }

    gramd = {
        key : val
        for key, val in gramd.items()
        if not any(punc in key for punc in punctuation)
    }
    
    return gramd

if __name__ == '__main__':

	reuters_tg = corpus_ngrams(reuters.sents(), 2, 50)
	brown_tg = corpus_ngrams(brown.sents(), 2, 1)
	gut_tg = corpus_ngrams(gutenberg.sents(), 2, 1)

	two_grams = reuters_tg.copy()
	two_grams.update(brown_tg)
	two_grams.update(gut_tg)

	df = pd.DataFrame(two_grams.keys(), columns = ['gram'])
	df.to_csv("data/two_grams.csv", index=False)