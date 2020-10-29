from os.path import expanduser, join, abspath
import sys
# Use the directory you unzipped the instant client to:
import os
os.chdir("/Users/orshulrufer/Downloads/instantclient_19_3 2")
import cx_Oracle
ORACLE_CONNECT = "schema/password@(DESCRIPTION=(SOURCE_ROUTE=OFF)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521)))(CONNECT_DATA=(SID=sid)(SRVR=DEDICATED)))"

orcl = cx_Oracle.connect(ORACLE_CONNECT)
print("Connected to Oracle: " + orcl.version)

sql = "select * from MYTABLE"
curs = orcl.cursor()
curs.execute(sql)

for row in curs:
    print(row)
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd

# Create Spark Session
spark = SparkSession.builder.appName("SQLBasicResultexample").config("spark.some.config.option", "some-value").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sqlContext = SQLContext(spark)

#dsn_tns = cx_Oracle.makedsn('Host Name', 'Port Number', service_name='Service Name') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.

# initialize list of list
data = [['tom', 10], ['nick', 15], ['juli', 14]]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=['Name', 'Age'])

s = spark.createDataFrame(df)
s.registerTempTable("people")
s.show()

sqlContext.sql("insert into mysparkdb/src select * from people")

