# -*- coding: utf-8 -*-
"""Copy of BDM_HW4_You.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1jGwVKQpU5SmQxANK3m2FUzgrcPdX99bV
"""

!pip install pyspark

import pyspark
import json
import csv
from datetime import datetime
from datetime import timedelta  
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, MapType, StringType, FloatType
from pyspark.sql.functions import split, col, substring, regexp_replace, explode

sc = pyspark.SparkContext()
spark = SparkSession(sc)

def mapday(s, v):
  date_1 = datetime.strptime(s[:10], '%Y-%m-%d')
  result = {}

  l = json.loads(v)

  for i in range(0,7):
    date = date_1 + timedelta(days=i)
    result[date] = l[i]

  return result

def low(x, y):
  diff = int(round(x-np.std(y)))
  return 0 if diff < 0 else diff

def high(x,y):
  diff = int(round(x+np.std(y)))
  return 0 if diff < 0 else diff

def median(values_list):
  med = int(np.median(values_list))
  return med

if __name__=='__main__':

  NAICS = [['452210','452311'],['445120'],['722410'],
         ['722511'],['722513'],['446110','446191'],['311811','722515'],
         ['445210','445220','445230','445291','445292','445299'],['445110']]

  files = ["test/big_box_grocers", "test/convenience_stores", "test/drinking_places", 
          "test/full_service_restaurants", "test/limited_service_restaurants", "test/pharmacies_and_drug_stores",
          "test/snack_and_bakeries", "test/specialty_food_stores", "test/supermarkets_except_convenience_stores"]

  udfExpand = F.udf(mapday, MapType(DateType(), IntegerType()))
  udfMedian = F.udf(median, IntegerType())
  udfLow    = F.udf(low, IntegerType())
  udfHigh   = F.udf(high, IntegerType())

  newdf = spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True)

  for i in range(len(NAICS)):
    df = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header= True) \
                   .where(F.col('naics_code').isin(NAICS[i]))
    

    newDF = newdf.join(df, (newdf.placekey == df.placekey) & (newdf.safegraph_place_id == df.safegraph_place_id), "inner")\
                .select('date_range_start','visits_by_day')\
                .withColumn('date', substring('date_range_start',1,10))\
                .drop('date_range_start')\
                .select(F.explode(udfExpand('date', 'visits_by_day')).alias('date', 'visits'))

    newDFF = newDF.filter((newDF.date > '2018-12-31') & (newDF.date < '2021-01-01') & (newDF.visits > 0))\
                  .groupBy('date')\
                  .agg(F.collect_list('visits').alias('visits'))\
                  .withColumn('year', substring('date',1,4))\
                  .withColumn('date', regexp_replace('date', '2019', '2020'))\
                  .withColumn('median', udfMedian('visits').alias('median'))\
                  .withColumn('low', udfLow('median', 'visits').alias('low'))\
                  .withColumn('high', udfHigh('median','visits').alias('high'))\
                  .select('year', 'date', 'median', 'low', 'high')\
                  .write.format("csv")\
                  .option("header","true")\
                  .save(files[i])