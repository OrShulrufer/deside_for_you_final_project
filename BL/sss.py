import gensim
from nltk.tokenize import word_tokenize
import pandas as pd
import string
import re

df = pd.read_csv("data/course_descriptions.csv")


# Getting list of descriptions
raw_documents = df["Description"].tolist()

words = []
for doc in raw_documents:
    words += re.split(r'\W+', doc)
words = [word.lower() for word in words]
print(words)


