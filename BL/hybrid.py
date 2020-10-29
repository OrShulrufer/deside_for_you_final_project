from BL.collaborative_filtering import CF
from BL.content_base_filtering import CBF
import pandas

class Hybrid:

    def get_courses(clf_matrix_df, df_courses, user, numer_of_courses_to_fetch=5):
        # get clf courses
        clf_list_of_courses, user_courses_list = CF.get_curses(clf_matrix_df, user)

        # get cbf courses
        cbf_courses_list = CBF.get_courses(user_courses_list, df_courses)

        # add all CBF courses to combine list
        combine_list_of_courses = list(cbf_courses_list)

        # add up to 5 CLF course to comine list
        index = 0
        for course in clf_list_of_courses:
            if index < 5:
                if course not in combine_list_of_courses:
                    combine_list_of_courses.append(course)
                    index += 1

        return combine_list_of_courses


