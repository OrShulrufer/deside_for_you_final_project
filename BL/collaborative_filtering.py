import BL.columns_names as columns_names

import pandas as pd


class CF:

    @staticmethod
    def get_curses(df, user_id):

        df1 = df.T

        # get list of courses that user took
        courses = df1.index[df1[user_id] == 1].tolist()
        print("user took courses")
        print(courses)
        df1 = df1.T

        # get matrix with only courses that user took
        df1 = df1[courses]

        # get users that have all courses that user have
        for column in courses:
            df1 = df1[df1[column] == 1]

        # get users list
        users_list = df1.index.values.tolist()

        # get all courses list
        all_courses = list(df.columns)
        # get courses that user did not do yet
        courses_list = list(set(all_courses).difference(courses))

        # get df with columns of courses that user did not have
        df = df[courses_list]
        df = df.T
        # get df with only relevant users
        df = df[users_list]
        df = df.T

        return CF.get_courses_prioreties(df, df.columns[df.eq(1).any()].tolist()), courses

    # get list of courses propertiesed descending by popularity of courses
    @staticmethod
    def get_courses_prioreties(df, courses_list):
        courses = []
        # get all recommendations to list of courses id
        #list_of_courses = df.columns.values.tolist()

        # make tuples of course id and number of people took it in cluster
        for course in courses_list:
                courses.append((course, (df[course] == 1).sum()))

        # sort tuples by course popularity
        courses = sorted(courses, key=lambda x: x[1], reverse=True)

        # from tuples to list of only courses id
        courses = list(map(lambda x: x[0], courses))

        return courses














