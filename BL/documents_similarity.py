import gensim
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import pandas as pd
import string
import nltk
nltk.download('punkt')
nltk.download('stopwords')



# class Description:
#
#     def getsss(self, df):

stop_words = set(stopwords.words('english'))
df = pd.read_csv("data/full_descriptions.csv")

print(df)
# Getting list of descriptions
index = 1

raw_documents = df["TopicsLearned"].map(lambda x: x.strip()).tolist()
#raw_documents.map(lambda x: x.strip())
print(raw_documents)
"""
wordcloud = WordCloud().generate(raw_documents[0])
plt.imshow(wordcloud)
plt.axis("off")
plt.show()
"""
# Getting list of course numbers
courses = df["course_number"].tolist()

"""
We will use NLTK to tokenize.
A document will now be a list of tokens.
"""
gen_docs = [[w.lower() for w in word_tokenize(text) if w not in stop_words]
            for text in raw_documents]
table = str.maketrans('', '', string.punctuation)
gen_docs = [[str(w).translate(table) for w in words] for words in gen_docs]

"""
We will create a dictionary from a list of documents.
 A dictionary maps every word to a number.
"""
dictionary = gensim.corpora.Dictionary(gen_docs)
print("dict: ", dictionary)

"""
Now we will create a corpus. 
A corpus is a list of bags of words. 
A bag-of-words representation for a document just lists the number of times each word occurs in the document.
Convert document (a list of words) into the bag-of-words format = list of (token_id, token_count) 2-tuples
"""
corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
print("corpus: ", corpus)

"""
Now we create a tf-idf model from the corpus.
Note that num_nnz is the number of tokens.
"""
tf_idf = gensim.models.TfidfModel(corpus)
print(tf_idf)


w2v_model = gensim.models.Word2Vec(gen_docs)
similarity_index = gensim.models.WordEmbeddingSimilarityIndex(w2v_model.wv)
similarity_matrix = gensim.similarities.SparseTermSimilarityMatrix(similarity_index, dictionary, tf_idf, nonzero_limit=100)


"""
Now we will create a similarity measure object in tf-idf space.
tf-idf stands for term frequency-inverse document frequency. 
Term frequency is how often the word shows up in the document and inverse document
fequency scales the value by how rare the word is in the corpus.
"""
"""
sims = gensim.similarities.MatrixSimilarity(tf_idf[corpus],
                                      num_features=len(dictionary))

"""

sims = gensim.similarities.SoftCosineSimilarity(tf_idf[corpus],
                                      similarity_matrix)


#print(sims)
print(type(sims))

"""
Now create a query document and convert it to tf-idf.
"""
"""
wordcloud = WordCloud().generate("6.002x is a fundamental undergraduate electrical engineering course that introduces engineering in the context of the lumped circuit abstraction. Topics covered include: resistive elements and networks; independent and dependent sources; switches and MOS transistors; digital abstraction; amplifiers; energy storage elements; dynamics of first- and second-order networks; design in the time and frequency domains; and analog and digital circuits and applications. Design and lab exercises are also significant components of the course. Materials taught in 6.002x are equivalent to those taught in 6.002. At MIT 6.002 is in the core of department subjects required for all undergraduates in Electrical Engineering and Computer Science.")
plt.imshow(wordcloud)
plt.axis("off")
plt.show()
"""

query_doc = [w.lower() for w in word_tokenize("6.002x is a fundamental undergraduate electrical engineering course that introduces engineering in the context of the lumped circuit abstraction. Topics covered include: resistive elements and networks; independent and dependent sources; switches and MOS transistors; digital abstraction; amplifiers; energy storage elements; dynamics of first- and second-order networks; design in the time and frequency domains; and analog and digital circuits and applications. Design and lab exercises are also significant components of the course. Materials taught in 6.002x are equivalent to those taught in 6.002. At MIT 6.002 is in the core of department subjects required for all undergraduates in Electrical Engineering and Computer Science.")]
print(query_doc)
query_doc_bow = dictionary.doc2bow(query_doc)
print(query_doc_bow)
query_doc_tf_idf = tf_idf[query_doc_bow]
print(query_doc_tf_idf)


"""
We show an array of document similarities to query. 
"""
print(sims[query_doc_tf_idf])









