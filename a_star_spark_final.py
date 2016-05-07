import os
import sys
os.environ['SPARK_HOME']="/Users/chhavi21/spark-1.6.0-bin-hadoop2.6/"
sys.path.append("/Users/chhavi21/spark-1.6.0-bin-hadoop2.6/python")

from pyspark import SparkConf, SparkContext, Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from math import radians, sin, cos, sqrt, asin
import time

# start_time = time()
conf = SparkConf().setAppName("a_star")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



def mapper(x):
    if len(x)==2: tpl, lst = x[0], x[1]
    else: return [x]
    if tpl[1] != 'INFINITY' and tpl[2] == 'UNDISCOVERED':
        if len(tpl) == 3:
            updated_path = tpl[0]
        else:
            updated_path = str(tpl[3]) + '-' + str(tpl[0])
        return_data = [((tpl[0], tpl[1], 'DISCOVERED', updated_path ), lst)]

        for l in lst:
            return_data.append((l[0], (tpl[1]+l[1]), 'UNDISCOVERED', updated_path))
        return return_data
    else:
        return [(tpl, lst)]


def reducer(a, b):
    if len(a) != 2:
        a = (a,) + ([],)
    if len(b) != 2:
        b = (b,) + ([],)
    if a[0][1] != 'INFINITY' and b[0][1] != 'INFINITY':
        if a[0][1] < b[0][1]:
            front = a[0]
            if a[1] != []:
                back = a[1]
            else:
                back = b[1]
        else:
            front = b[0]
            if b[1] != []:
                back = b[1]
            else:
                back = a[1]
    elif a[0][1] == 'INFINITY':
        front = b[0]
        if b[1] != []:
            back = b[1]
        else:
            back = a[1]
    elif b[0][1] == 'INFINITY':
        front = a[0]
        if a[1] != []:
            back = a[1]
        else:
            back = b[1]
    return (front, back)


def return_key(a):
    if len(a)==2:
        return a[0][0]
    else:
        return a[0]



coordinate = sc.textFile("USA-road-d.BAY.co").repartition(16) #321270
time = sc.textFile("USA-road-t.BAY.gr").repartition(16) # 800172

coordinate = coordinate.map(lambda x: x.split(' ')).map(lambda x: (x[1], float(x[3])/1000000, float(x[2])/1000000))
time = time.map(lambda x: x.split(' ')).map(lambda x: (x[1],x[2],int(x[3])))


def combine(a, b):
    return list(a) + list(b)

def get_data_ready(x):
    if x[0] == s:
        return ((x[0], 0, 'UNDISCOVERED'), x[1])
    else:
        return((x[0], 'INFINITY', 'UNDISCOVERED'), x[1])

#define start point
s=1
e=4

#read data
distance = sc.textFile("USA-road-d.BAY.gr").repartition(16) # 800172
distance = distance.map(lambda x: x.split(' ')).map(lambda x: (int(x[1]),int(x[2]),float(x[3])/10))

#put in the rigth format
distance = distance.map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(lambda a,b: combine(a,b))
distance = distance.map(lambda x: get_data_ready(x))


import time
start_time = time.time()
while True:
    # map
    distance = distance.flatMap(lambda x: mapper(x))
    # reduce
    distance = distance.map(lambda x: (return_key(x), x)).reduceByKey(lambda a,b: reducer(a,b)).map(lambda (a,b): b)
    check = distance.map(lambda x: (return_key(x), x)).filter(lambda x: x[0]==e).collect()
    if check[0][1][0][2] == 'DISCOVERED':
        break

print (time.time() - start_time)/60, " mins"

path  = check[0][1][0][-1]
tot_distance = check[0][1][0][1]

