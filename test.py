import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingeltonInstace' not in globals()):
        globals()['sqlContextSingeltonInstace'] = SQLContext(sparkContext)
    return globals()['sqlContextSingeltonInstace']

if __name__ == "__main__":
    if len(sys.arg) != 2:
        print("Usage uberstats <file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="uberstats")
    sc.stop()
