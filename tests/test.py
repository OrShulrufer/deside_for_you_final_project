import BL.columns_names as columns_names


class Test:

    def getCoursesOfUser(self, user_id, df, spark):

        df.registerTempTable("df")

        return spark.sql("SELECT DISTINCT course_id "
                         "FROM df "
                         "WHERE userid_DI = '{}'".format(user_id)).toPandas()[columns_names.course_id].to_list()

'''
    ['CS50x', '8.02x', '6.00x', '3.091x', '14.73x', '6.002x', 'PH278x', 'CB22x', '2.01x', '8.MReV', 'ER22x', 'PH207x',
     '7.00x']
     
     ['HarvardX/CS50x/2012', 'MITx/6.002x/2012_Fall', 'MITx/2.01x/2013_Spring', 'MITx/3.091x/2013_Spring', 'MITx/8.02x/2013_Spring', 'HarvardX/ER22x/2013_Spring', 'MITx/7.00x/2013_Spring', 'MITx/6.00x/2012_Fall', 'HarvardX/PH278x/2013_Spring']


'''
