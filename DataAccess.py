from sqlalchemy import create_engine
import pandas as pd
from config import DB_URL,DB_USER,DB_PASSWORD
pymysql_engine_url = "mysql+pymysql://"+DB_USER+":"+DB_PASSWORD+"@"+DB_URL

class DataAccess:
    def __init__(self, name, age):
        self.name = name
        self.age = age


class DataHelper:
    def fetch_df_by_query(query):
        engine = create_engine(pymysql_engine_url);
        df = pd.read_sql(query, con=engine)
        return df


    def fetch_df_from_large_sql_data(spark,push_down_query,partition_column,num_partitions=1,lower_bound=1,upper_bound=10000):
        driver = "com.mysql.jdbc.Driver";
        dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://"+DB_URL).option("driver", driver).\
        option("dbtable", push_down_query).option("user", DB_USER).option("password", DB_PASSWORD).\
        option("numPartitions", num_partitions).option("lowerBound", lower_bound).option("upperBound", upper_bound).option("partitionColumn", partition_column).load()
        #set numPartitions to 70 when student_course?
        return dataframe_mysql


    def insert_from_df(data_df,table_name):
        '''
        example dataframe
        data_df = {"UserId":["xxxxx", "yyyyy", "zzzzz", "aaaaa", "bbbbb", "ccccc", "ddddd"],

                "UserFavourite":["Greek Salad", "Philly Cheese Steak", "Turkey Burger", "Crispy Orange Chicken", "Atlantic Salmon", "Pot roast", "Banana split"],

                "MonthlyOrderFrequency":[5, 1, 2, 2, 7, 6, 1]}
        '''
        engine = create_engine(pymysql_engine_url)
        df = pd.DataFrame(data_df)
        df.to_sql(table_name,con=engine,if_exists='append', index=False)


    def query_from_df(query,data_panda_DF=None):
        engine = create_engine(pymysql_engine_url)
        if data_panda_DF is not None:
            data_panda_DF.to_sql('temp_table', engine, if_exists='replace', index=False)
        with engine.begin() as conn:
            conn.execute(query)
            '''
            example update query
            UPDATE tableB
            INNER JOIN tableA ON tableB.name = temp_table.name
            SET tableB.value = IF(temp_table.value > 0, temp_table.value, tableB.value)
            WHERE temp_table.name = 'Joe'
            '''
