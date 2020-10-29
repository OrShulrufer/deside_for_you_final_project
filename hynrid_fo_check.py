from BL.collaborative_filtering import CF
from BL.content_base_filtering import CBF
import pandas

class Hybrid:

    def get_courses(clf_matrix_df, df_clusters, df_courses, user_course_df, user, stop_words, numer_of_courses_to_fetch=5):
        # fetch cluster number by_user
        cluster_number = df_clusters.loc[df_clusters['userid_DI'] == user, 'prediction'].iloc[0]

        # fetch cluster by cluster number
        cluster_df = df_clusters
        #cluster_df = df_clusters.loc[df_clusters['prediction'].isin([cluster_number])]

        # get clf matrix only with cluster of user
        users_list = clf_matrix_df.index.unique()
        combine_list = list(set(users_list).intersection(cluster_df.userid_DI.unique()))
        # combine_list = list(set(users_list).intersection(user_course_df["userid_DI"].unique()))
        cluster_matrix_df = clf_matrix_df[clf_matrix_df.index.isin(combine_list)]

        # get clf courses
        clf_list_of_courses, users_courses = CF.get_curses(cluster_matrix_df, user)
        # get cbf courses
        cbf_corses_list = CBF.get_courses(users_courses, df_courses, stop_words)
        #cbf_corses_list = []

        combine_list_of_courses = list(cbf_corses_list)
        """
        index = 0
        for course in clf_list_of_courses:
            if index < 5:
                if course not in combine_list_of_courses:
                    combine_list_of_courses.append(course)
                    index += 1
        """

       # combine_list_of_courses = list(set(clf_list_of_courses[:5] + list(cbf_corses_list)))

        return combine_list_of_courses

