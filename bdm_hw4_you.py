import pyspark
import sys
import json
import csv
from datetime import datetime
from datetime import timedelta  
import sys
import statistics

def strTolist(x):
  ans = []
  temp = ''

  for i in range(0, len(x)):
    if x[i].isdigit():
      temp = temp + x[i]
    elif not x[i].isdigit() and temp:
      ans.append(int(temp))
      temp = ''

  return ans

def mapday(s, v):
  date_1 = datetime.strptime(s, '%Y-%m-%d')

  result = ()

  for i in range(0,7):
    date = date_1 + timedelta(days=i)
    result += (str(date)[:10], v[i]),

  return result

def low(x, y):
  diff = x-statistics.pstdev(y)
  if diff < 0:
    return 0
  else:
    return int(round(diff))

def high(x,y):
  diff = x+statistics.pstdev(y)
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

  sc = pyspark.SparkContext()

  data = []
  category = []

  for i in range(len(NAICS)):
    data.append(sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
                  .filter(lambda x: next(csv.reader([x]))[9] in NAICS[i])\
                  .cache()
              )
    category.append(data[i] \
                    .map(lambda x: next(csv.reader([x])))\
                    .map(lambda x: (x[0],x[1]))
                  )
    
  new_category = []
  for i in range(len(category)):
    new_category.append(category[i].collect())

  new_data = []
  weekly_data = []

  for i in range(len(NAICS)):
    new_data.append(sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*') \
                      .filter(lambda x: tuple(next(csv.reader([x]))[0:2]) in new_category[i])\
                      .cache()
                  )
    weekly_data.append(new_data[i]\
                      .map(lambda x: next(csv.reader([x])))\
                      .map(lambda x: (x[12][:10],strTolist(x[16])))
                      )

  for i in range(0,len(NAICS)):
    weekly_data[i]\
      .flatMap(lambda x : mapday(x[0],x[1]))\
      .filter(lambda x: x[1] != 0 and x[0] > '2018-12-31' and x[0] < '2021-01-01')\
      .groupByKey() \
      .mapValues(list)\
      .sortBy(lambda x: x[0])\
      .map(lambda x: (x[0][:4], x[0], int(round(statistics.median(x[1]))), low(statistics.median(x[1]), x[1]), high(statistics.median(x[1]), x[1])))\
      .saveAsTextFile(files[i])