import pyspark
import sys
import json
import csv
from datetime import datetime
from datetime import timedelta  
import sys
import statistics

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

  for i in range(0,len(NAICS)):
    data.append(sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')\
                  .filter(lambda x: next(csv.reader([x]))[9] in NAICS[i])\
                  .cache()
              )
    data[i].map(lambda x: next(csv.reader([x])))\
           .map(lambda x: (x[0],x[1]))\
           .saveAsTextFile(files[i])