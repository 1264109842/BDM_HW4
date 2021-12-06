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
from pyspark.sql.types import DateType, IntegerType, MapType, StringType, ArrayType
from pyspark.sql.functions import split, col, substring, regexp_replace, explode, broadcast

sc = pyspark.SparkContext()
spark = SparkSession(sc)

NAICS = {"/big_box_grocers" : {'452210','452311'},
          "/convenience_stores" : {'445120'},
          "/drinking_places" : {'722410'},
        "/full_service_restaurants" : {'722511'}, 
        "/limited_service_restaurants" : {'722513'}, 
        "/pharmacies_and_drug_stores" : {'446110','446191'},
        "/snack_and_bakeries" : {'311811','722515'},
        "/specialty_food_stores" : {'445210','445220','445230','445291','445292','445299'},
        "/supermarkets_except_convenience_stores" : {'445110'}}

files = ["/big_box_grocers", "/convenience_stores", "/drinking_places",
        "/full_service_restaurants", "/limited_service_restaurants", "/pharmacies_and_drug_stores",
        "/snack_and_bakeries", "/specialty_food_stores", "/supermarkets_except_convenience_stores"]

naics = set.union(*NAICS.values())

def mapday(s, v):
  date_1 = datetime.strptime(s[:10], '%Y-%m-%d')
  result = {}

  l = json.loads(v)

  for i in range(0,7):
    if l[i] == 0: continue
    date = date_1 + timedelta(days=i)
    if date.year in (2019, 2020):
      result[date] = l[i]

  return result

def median(values_list):
  med = np.median(values_list)
  stdev = np.std(values_list)
  return [int(med), max(0, int(med-stdev+0.5)), int(med+stdev+0.5)]

def setNaics(n):
  for a, b in enumerate(NAICS.values()):
    if n in b:
      return a


udfExpand = F.udf(mapday, MapType(DateType(), IntegerType()))
udfMedian = F.udf(median, ArrayType(IntegerType()))
udfNaics  = F.udf(setNaics, IntegerType())
  
if __name__=='__main__':

  newdf = spark.read.csv('weekly_pattern', header=True)
  new = spark.read.csv('core-places-nyc.csv', header= True)

  df = new.filter(F.col('naics_code').isin(*naics))\
          .select('placeKey', udfNaics('naics_code').alias('Group')).cache()

  newDF = newdf.join(broadcast(df), (newdf.placekey == df.placeKey))\
                .select('Group', F.explode(udfExpand('date_range_start', 'visits_by_day')).alias('date', 'visits')).cache()
  
  newDFF = newDF.groupBy('Group','date')\
          .agg(F.collect_list('visits').alias('visits'))\
          .withColumn('median', udfMedian('visits'))\
          .withColumn('year', substring('date',1,4))\
          .withColumn('date',regexp_replace('date', '2019', '2020'))\
          .orderBy('year','date')\
          .coalesce(1)\
          .cache()

  for i, j in enumerate(NAICS):
    newDFF.filter(F.col('Group') == i)\
          .select('year', 'date', newDFF.median[0].alias('median'), newDFF.median[1].alias('low'), newDFF.median[2].alias('high'))\
          .write.csv('test'+list(NAICS.keys())[i], mode='overwrite', header=True)