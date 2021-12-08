# -*- coding: utf-8 -*-
"""BDM_HW4_df.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1fy1zulJFyWjmfgpS1zAHzkcLtKiiXoxX
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
import json
import numpy as np
import sys

def main(sc, spark):
    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    dfPlaces = spark.read.csv('/data/share/bdm/core-places-nyc.csv', header=True, escape='"')
    dfPattern = spark.read.csv('/data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')
    OUTPUT_PREFIX = sys.argv[1]

    CAT_CODES = {'445210', '722515', '445299', '445120', '452210', '311811', '722410', '722511', '445220', '445292', '445110', '445291', '445230', '446191', '446110', '722513', '452311'}
    CAT_GROUP = {'452311': 0, '452210': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446191': 5, '446110': 5, '722515': 6, '311811': 6, '445299': 7, '445220': 7, '445292': 7, '445291': 7, '445230': 7, '445210': 7, '445110': 8}

    files = ["big_box_grocers", "convenience_stores", "drinking_places",
            "full_service_restaurants", "limited_service_restaurants", "pharmacies_and_drug_stores",
            "snack_and_bakeries", "specialty_food_stores", "supermarkets_except_convenience_stores"]

    udfToGroup = F.udf(lambda x: CAT_GROUP[x] , T.IntegerType())

    dfD = dfPlaces. \
                  filter(F.col('naics_code').isin(CAT_CODES))\
                  .withColumn('group', udfToGroup('naics_code'))\
                  .select('placekey', 'group')\
                  .cache()
    
    groupCount = dict(dfD.groupBy('group')\
                .count()\
                .collect())
    
    
    visitType = T.StructType([T.StructField('year', T.IntegerType()),
                          T.StructField('date', T.StringType()),
                          T.StructField('visits', T.IntegerType())])
    
    statsType = T.StructType([T.StructField('median', T.IntegerType()),
                          T.StructField('low', T.IntegerType()),
                          T.StructField('high', T.IntegerType())])
    
    def expandVisits(date_range_start, visits_by_day):
      date_1 = datetime.datetime.strptime(date_range_start[:10], '%Y-%m-%d')
      result = []

      l = json.loads(visits_by_day)

      for i in range(len(l)):
        if l[i] == 0: continue
        date = date_1 + datetime.timedelta(days=i)
        if date.year in (2019, 2020):
          result.append((int(date.year), str(date)[5:10], l[i]))

      return result

    def computeStats(group, visits):
      visits = np.fromiter(visits, np.int_)
      visits.resize(groupCount[group])
      med = np.median(visits)
      stdev = np.std(visits)
      return (int(med), max(0, int(med-stdev+0.5)), int(med+stdev+0.5))

    udfExpand = F.udf(expandVisits, T.ArrayType(visitType))
    udfComputeStats = F.udf(computeStats, statsType)

    dfH = dfPattern.join(dfD, 'placekey') \
                  .withColumn('expanded', F.explode(udfExpand('date_range_start', 'visits_by_day'))) \
                  .select('group', 'expanded.*')\
                  .groupBy('group', 'year', 'date') \
                  .agg(F.collect_list('visits').alias('visits')) \
                  .withColumn('stats', udfComputeStats('group', 'visits'))\
                  .select('group', 'year', 'date', 'stats.*')\
                  .orderBy('group', 'year', 'date')\
                  .withColumn('date', F.concat(F.lit('2020-'), F.col('date')))\
                  .coalesce(1)\
                  .cache()

    for i in range(9):
      dfH.filter(F.col('group') == i) \
          .drop('group') \
          .coalesce(1) \
          .write.csv(f'{OUTPUT_PREFIX}/{files[i]}',
                    mode='overwrite', header=True)

if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    main(sc, spark)