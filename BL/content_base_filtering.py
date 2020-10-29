from pyspark.sql import SparkSession
import BL.columns_names as columns_names
from pyspark.sql import functions as F

import gensim
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
import string
import numpy as np



class CBF:
    @staticmethod
    def get_courses(user_courses_list, df_courses, stop_words=None):

        df_courses["combine"] = df_courses["Description"]

        dictionary, stop_words, tf_idf, cosine_similarity_index = CBF.get_cosine_similarity_index(df_courses, stop_words)

        # get 2 recommendations for every course user took
        recomendet_courses = set()
        for course in user_courses_list:
            recomendet_courses |= set(CBF.get_recomendation_by_course_number(
                df_courses, course, cosine_similarity_index, dictionary, stop_words, tf_idf))

        return recomendet_courses

    @staticmethod
    def get_recomendation_by_course_number(df_courses, course, cosine_similarity_index, dictionary, stop_words, tf_idf):

        description = df_courses.loc[df_courses['course_number'] == course, 'combine'].iloc[0]
        """
        We will use NLTK to tokenize.
        A document will now be a list of tokens.
        """
        query_doc = [w.lower() for w in word_tokenize(description) if w not in stop_words]
        table = str.maketrans('', '', string.punctuation)
        query_doc_without_punctuations = [str(w).translate(table) for w in query_doc]

        # create a bag-of-words
        query_doc_bow = dictionary.doc2bow(query_doc_without_punctuations)

        # create tfidf of query description
        query_doc_tf_idf = tf_idf[query_doc_bow]

        # get similarity evaluations for ather courses
        similarities = cosine_similarity_index[query_doc_tf_idf]

        # find the most similar courses indexes
        indexes_of_nearest_courses = np.argpartition(similarities, -3)[-3:][:2]
        course_list = df_courses["course_number"].to_list()

        best_courses = [course_list[i] for i in indexes_of_nearest_courses]

        return best_courses


    @staticmethod
    def get_cosine_similarity_index(df, stop_words):
        nltk.download('punkt')
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

        # df.sort_values(by=['course_number'], ascending=False)

        # Getting list of descriptions
        raw_documents = df["combine"].to_list()

        """
        We will use NLTK to tokenize.
        A document will now be a list of tokens.
        """
        gen_docs = [[w for w in word_tokenize(text) if w not in stop_words]
                    for text in raw_documents if text is not None]
        table = str.maketrans('', '', string.punctuation)
        gen_docs = [[str(w).translate(table) for w in words] for words in gen_docs]

        """
        We will create a dictionary from a list of documents.
         A dictionary maps every word to a number.
        """
        dictionary = gensim.corpora.Dictionary(gen_docs)

        """
        Now we will create a corpus. 
        A corpus is a list of bags of words. 
        A bag-of-words representation for a document just lists the number of times each word occurs in the document.
        Convert document (a list of words) into the bag-of-words format = list of (token_id, token_count) 2-tuples
        """
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]

        """
        Now we create a tf-idf model from the corpus.
        Note that num_nnz is the number of tokens.
        """
        tf_idf = gensim.models.TfidfModel(corpus)

        # creating cosine similarity index
        cosine_similarity_index = gensim.similarities.MatrixSimilarity(tf_idf[corpus], num_features=len(dictionary))

        return dictionary, stop_words, tf_idf, cosine_similarity_index



































