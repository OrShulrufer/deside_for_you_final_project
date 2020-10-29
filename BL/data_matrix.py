import pandas as pd


class DataMatrix:
    @staticmethod
    def get_data_matrix(df_user_course):

        # get set of courses
        courses_temp = df_user_course['course_id'].unique()

        # get set of users
        users_ids = df_user_course['userid_DI'].unique()

        # Create the pandas DataFrame full with zeros
        df = pd.DataFrame(index=users_ids, columns=courses_temp).fillna(0)

        # get only course and users columns
        df_courses = df_user_course[['course_id', 'userid_DI']]

        for c in courses_temp:
            # get all users that took this class
            users = df_courses[df_courses['course_id'] == c]['userid_DI'].unique().tolist()

            # fill matrix with 1 where users took thus course
            list_of_ones = [1] * len(users)
            new_column = pd.Series(list_of_ones, name=c, index= users)
            df.update(new_column)

        # make matrix only with ints for faster communication with MySql
        for col in df.columns.values:
            df[col] = df[col].astype('int64')

        return df







