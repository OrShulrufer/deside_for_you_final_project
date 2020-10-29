import gensim
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import pandas as pd
import string
import nltk




nltk.download('punkt')
nltk.download('stopwords')


stop_words = set(stopwords.words('english'))
df = pd.read_csv("data/course_descriptions.csv")

# Getting list of descriptions
raw_documents = df["Description"].tolist()

# Getting list of course numbers
courses = df["course_number"].tolist()
print(raw_documents)

"""
We will use NLTK to tokenize.
A document will now be a list of tokens.
"""
gen_docs = [[w.lower() for w in word_tokenize(text) if w not in stop_words]
            for text in raw_documents if text is not None]
table = str.maketrans('', '', string.punctuation)
gen_docs = [[str(w).translate(table) for w in words] for words in gen_docs]

"""
We will create a dictionary from a list of documents.
 A dictionary maps every word to a number.
"""
dictionary = gensim.corpora.Dictionary(gen_docs)

tfidf = gensim.models.TfidfModel(dictionary=dictionary)
w2v_model = gensim.models.Word2Vec(gen_docs)
similarity_index = gensim.models.WordEmbeddingSimilarityIndex(w2v_model.wv)
similarity_matrix = gensim.similarities.SparseTermSimilarityMatrix(similarity_index, dictionary, tfidf, nonzero_limit=100)

corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs];

sims = gensim.similarities.MatrixSimilarity(tfidf[corpus],
                                     num_features=len(dictionary))

query_doc = [w.lower() for w in word_tokenize("6.002x is a fundamental undergraduate electrical engineering course that introduces engineering in the context of the lumped circuit abstraction. Topics covered include: resistive elements and networks; independent and dependent sources; switches and MOS transistors; digital abstraction; amplifiers; energy storage elements; dynamics of first- and second-order networks; design in the time and frequency domains; and analog and digital circuits and applications. Design and lab exercises are also significant components of the course. Materials taught in 6.002x are equivalent to those taught in 6.002. At MIT 6.002 is in the core of department subjects required for all undergraduates in Electrical Engineering and Computer Science.")]
print(query_doc)
query_doc_bow = dictionary.doc2bow(query_doc)
print(query_doc_bow)
query_doc_tf_idf = tfidf[query_doc_bow]
print(query_doc_tf_idf)
print(sims[query_doc_tf_idf])

'''
def softcossim(query, documents):
    # Compute Soft Cosine Measure between the query and the documents.
    query = tfidf[dictionary.doc2bow(query)]
    index = gensim.similarities.SoftCosineSimilarity(
        tfidf[[dictionary.doc2bow(document) for document in documents]],
        similarity_matrix)
    similarities = index[query]
    return similarities

def crossvalidate(args):
    # Perform a cross-validation.
    test_data = np.array(list(produce_test_data(dataset)))
    kf = KFold(n_splits=10)
    samples = []
    for _, test_index in kf.split(test_data):
        samples.append(evaluate(test_data[test_index]))
    return (np.mean(samples, axis=0), np.std(samples, axis=0))




def evaluate(split):
    # Perform a single round of evaluation.
    results = []
    start_time = time()
    for query, documents, relevance in split:
        similarities = softcossim(query, documents)
        assert len(similarities) == len(documents)
        precision = [
            (num_correct + 1) / (num_total + 1) for num_correct, num_total in enumerate(
                num_total for num_total, (_, relevant) in enumerate(
                    sorted(zip(similarities, relevance), reverse=True)) if relevant)]
        average_precision = np.mean(precision) if precision else 0.0
        results.append(average_precision)
    return (np.mean(results) * 100, time() - start_time)
'''
