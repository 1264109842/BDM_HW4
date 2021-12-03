# -*- coding: utf-8 -*-
"""Copy of BDM_HW4_You.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1jGwVKQpU5SmQxANK3m2FUzgrcPdX99bV
"""

# !pip install pyspark

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

def mapday(s, v):
  date_1 = datetime.strptime(s[:10], '%Y-%m-%d')
  result = {}

  l = json.loads(v)

  for i in range(0,7):
    date = date_1 + timedelta(days=i)
    result[date] = l[i]

  return result

# def median(values_list):
#   med = np.median(values_list)
#   stdev = np.std(values_list)
#   low = int(med-stdev)
#   high = int(med+stdev)

#   result = []

#   result.append(int(med))
#   result.append(low) if low > 0 else result.append(0)
#   result.append(high) if high > 0 else result.append(0)

#   return result

  
if __name__=='__main__':

  udfExpand = F.udf(mapday, MapType(DateType(), IntegerType()))
  # udfMedian = F.udf(median, ArrayType(IntegerType()))
  udfMedian = F.udf(lambda x: int(np.median(x)), IntegerType())

  NAICS = [['452210','452311'],['445120'],['722410'],
         ['722511'],['722513'],['446110','446191'],['311811','722515'],
         ['445210','445220','445230','445291','445292','445299'],['445110']]

  TNAICS = ['452210','452311','445120','722410',
         '722511','722513','446110','446191','311811','722515',
         '445210','445220','445230','445291','445292','445299','445110']

  files = ["/big_box_grocers", "/convenience_stores", "/drinking_places",
          "/full_service_restaurants", "/limited_service_restaurants", "/pharmacies_and_drug_stores",
          "/snack_and_bakeries", "/specialty_food_stores", "/supermarkets_except_convenience_stores"]

  newdf = spark.read.csv('weekly_pattern', header=True)
  new = spark.read.csv('core-places-nyc.csv', header= True)

  for i in range(len(NAICS)):
    df = new.filter(F.col('naics_code').isin(NAICS[i]))

    newDF = newdf.join(broadcast(df), (newdf.placekey == df.placekey))\
                  .select(F.explode(udfExpand('date_range_start', 'visits_by_day')).alias('date', 'visits'))

    # newDFF = newDF.filter((newDF.date > '2018-12-31') & (newDF.date < '2021-01-01') & (newDF.visits > 0))\
    #               .groupBy('date')\
    #               .agg(F.collect_list('visits').alias('visits'))\
    #               .withColumn('median', udfMedian('visits'))\
    #               .withColumn('year', substring('date',1,4))\
    #               .withColumn('date',regexp_replace('date', '2019', '2020'))\
    #               .orderBy('year','date')

    newDFF = newDF.where((newDF.date > '2018-12-31') & (newDF.visits > 0))\
                  .groupBy('date')\
                  .agg(F.collect_list('visits').alias('visits'),F.stddev('visits').cast('int').alias('stddev'))\
                  .withColumn('median', udfMedian('visits'))\
                  .na.fill(0)\

    newDFFF = newDFF.withColumn('low',  F.when(newDFF.median - newDFF.stddev < 0, 0).otherwise((newDFF.median - newDFF.stddev)))\
                    .withColumn('high',  (newDFF.median + newDFF.stddev))\
                    .withColumn('year', substring('date',1,4))\
                    .withColumn('date', regexp_replace('date', '2019', '2020'))\
                    .orderBy('year', 'date')
                    
    newDFFF.select('year', 'date', 'median', 'low', 'high')\
                    .coalesce(1)\
                    .write.format("csv")\
                    .option("header","true")\
                    .save('test'+files[i])