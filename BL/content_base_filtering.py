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

    def get_courses(self, df_students, df_courses, user_id, spark):
        print(df_courses)
        # register data frame as table for using SQL
        df_students.registerTempTable("students")
        df_courses.registerTempTable("courses")

        # get all user's courses from the past
        user_courses = spark.sql("SELECT DISTINCT course_id "
                                    "FROM students "
                                    "WHERE userid_DI = '{}'".
                                    format(user_id)).toPandas()[columns_names.course_id].to_list()

        dictionary, stop_words, tf_idf, cosine_similarity_index = self.get_cosine_similarity_index(df_courses)

        # get 2 recommendations for every course user took
        recomendet_courses = set()
        for course in user_courses:
            recomendet_courses |= set(self.get_recomendation_by_course_number(
                df_courses, course, cosine_similarity_index, dictionary, stop_words, tf_idf, spark))

        return recomendet_courses


    def get_recomendation_by_course_number(
            self, df_courses, course, cosine_similarity_index, dictionary, stop_words, tf_idf, spark):

        # register data frame as table for using SQL
        df_courses.registerTempTable("courses")

        # get course description
        description = spark.sql("SELECT Description "
                                 "FROM courses "
                                 "WHERE course_number = '{}'".
                                 format(course)).toPandas()["Description"].to_list()[0]

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
        indexes_of_nearest_courses = np.argpartition(similarities, -3)[-3:][0:2]

        course_list = df_courses.toPandas()["course_number"].to_list()

        best_courses = [course_list[i] for i in indexes_of_nearest_courses]
        print("best courses for: ", course, " is ", best_courses)
        return best_courses


    def get_cosine_similarity_index(self, df):
        nltk.download('punkt')
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

        # Getting list of descriptions
        raw_documents = df.toPandas()["Description"].to_list()

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



































