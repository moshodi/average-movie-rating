# Databricks notebook source
from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName("Average")
context = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = context.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1]), 1))).reduceByKey(lambda x,y: (sum([x[0],y[0]]),sum([x[1],y[1]])))
rdd2.collect()

# COMMAND ----------

rdd2 = rdd2.map(lambda x: (x[0],x[1][0]/x[1][1])).map(lambda x: (x[0], round(x[1], 2)))
rdd2.collect()

# COMMAND ----------


