import pandas as pd
from flask import Flask
from DataAccess import get_users_demogr_details_df
from DataAccess import get_all_course_id_student_id_panda
from DataAccess import get_all_course_id_student_id

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from BL.create_clusters import Cluster
from BL.prepare_data_spark import Data
from BL.data_matrix import DataMatrix
from BL.collaborative_filtering import CF
from csv import writer
import DataAccess
from ast import literal_eval
import os
from BL.content_base_filtering import CBF
from nltk.corpus import stopwords
import nltk
from DataAccess import get_courses

class CheckCBF:

    def check_CBF(int):
        nltk.download('punkt')
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

        train_df = pd.read_csv("train_students_course.csv")

        val_df = pd.read_csv("val_students_course.csv")
        clf_matrix_df = DataMatrix.get_data_matrix(train_df)

        train_list = train_df["userid_DI"].unique()
        val_list = val_df["userid_DI"].unique()
        userslist = list(set(val_list).intersection(train_list))

        userslist = userslist[:1000]

        courses_df = get_courses()
        """
        for user in userslist:
            print(user)
            user_courses = train_df.loc[train_df['userid_DI'] == user]["course_id"].unique()
            print(user_courses)
            courses = CBF.get_courses(user_courses, courses_df, stop_words)
            print(courses)
            x = 0
            y = 0
            val_courses = val_df.loc[val_df['userid_DI'] == user]["course_id"].unique()
            print(val_courses)
            for course in val_courses:
                print(course)
                if course in courses:
                    print("x")
                    x += 1
                else:
                    y += 1
                    print("y")
            """
        for user in userslist:
            print(user)
            user_courses = train_df.loc[train_df['userid_DI'] == user]["course_id"].unique()
            courses = CBF.get_courses(user_courses, courses_df, stop_words)
            print(courses)
            x = 0
            y = 0
            val_courses = courses
            print(val_courses)
            for course in val_courses:
                print(course)
                if course in courses:
                    print("x")
                    x += 1
                else:
                    y += 1
                    print("y")
            print(x)
            print(y)
            CheckCBF.append_list_as_row("CBF_course3.csv", [user, courses, x, y])

        header_list = ["user", "num_of_courses_from_cbf", "yes", "no",]
        df = pd.read_csv("CBF_course3.csv", names=header_list)

        yes = df['yes'].sum()
        no = df['no'].sum()

        presantage = (yes / (yes + no)) * 100
        print(presantage)

        os.remove("CBF_course3.csv")

    def append_list_as_row(file_name, list_of_elem):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            csv_writer = writer(write_obj)
            # Add contents of list as last row in the csv file
            csv_writer.writerow(list_of_elem)
