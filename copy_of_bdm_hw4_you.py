# -*- coding: utf-8 -*-
"""Copy of BDM_HW4_You.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1jGwVKQpU5SmQxANK3m2FUzgrcPdX99bV
"""

# !pip install pyspark

import pyspark
import sys
import json
import csv
from datetime import datetime
from datetime import timedelta  
import sys
import statistics
from pyspark.sql import SparkSession
from pyspark import SparkContext


sc = pyspark.SparkContext()

spark = SparkSession(sc)

def mapday(s, v):
  date_1 = datetime.strptime(s, '%Y-%m-%d')

  result = ()

  for i in range(0,7):
    date = date_1 + timedelta(days=i)
    if v[i] != 0:
      result += (str(date)[:10], v[i]),

  return result

def low(x, y):
  diff = x-y
  if diff < 0:
    return 0
  else:
    return int(round(diff))

def high(x,y):
  diff = x+y
  if diff < 0:
    return 0
  else:
    return int(round(diff))



if __name__=='__main__':

  NAICS = [['452210','452311'],['445120'],['722410'],
         ['722511'],['722513'],['446110','446191'],['311811','722515'],
         ['445210','445220','445230','445291','445292','445299'],['445110']]
  files = ["test/big_box_grocers", "test/convenience_stores", "test/drinking_places", 
          "test/full_service_restaurants", "test/limited_service_restaurants", "test/pharmacies_and_drug_stores",
          "test/snack_and_bakeries", "test/specialty_food_stores", "test/supermarkets_except_convenience_stores"]

  data = []
  new_data = []

  for i in range(len(NAICS)):
    data.append(sc.textFile('core-places-nyc.csv')\
                  .filter(lambda x: next(csv.reader([x]))[9] in NAICS[i])\
                  .map(lambda x: next(csv.reader([x])))\
                  .map(lambda x: (x[0],x[1]))\
                  .cache()\
                  .collect()
              )

    new_data.append(sc.textFile('weekly_pattern') \
                      .filter(lambda x: tuple(next(csv.reader([x]))[0:2]) in data[i])\
                      .map(lambda x: next(csv.reader([x])))\
                      .map(lambda x: (x[12][:10],json.loads(x[16])))\
                      .flatMap(lambda x : mapday(x[0],x[1]))\
                      .filter(lambda x: x[0] > '2018-12-31' and x[0] < '2021-01-01')\
                      .groupByKey() \
                      .mapValues(list)\
                      .sortBy(lambda x: x[0])\
                      .map(lambda x: (x[0][:4], "2020"+x[0][4:], int(round(statistics.median(x[1]))), statistics.pstdev(x[1])))
                  )
    
    new_data[i].map(lambda x: (x[0], x[1], x[2], low(x[2], x[3]), high(x[2], x[3])))\
                  .coalesce(1)\
                  .map(lambda x: (x[0],x[1],x[2],x[3],x[4]))\
                  .toDF(["year", "date" , "median","low","high"])\
                  .write.format("csv")\
                  .option("header", "true")\
                  .save(files[i])