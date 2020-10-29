from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import datetime
import columns_names
import pandas as pd


data = pd.read_csv("data/students.csv")

test = data.iloc[::5, :]
print(test)
test.to_csv("data/test.csv")

val = data.iloc[2::5, :]
print(val)
val.to_csv("data/val.csv")

print(data)

train1 = data.iloc[1::5, :]
train2 = data.iloc[3::5, :]
train3 = data.iloc[4::5, :]
train = pd.concat([train1, train2, train3]).sort_index().reset_index(drop=True)
train.to_csv("data/train.csv")
print(train)

