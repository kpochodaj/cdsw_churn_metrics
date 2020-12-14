# https://docs.cloudera.com/machine-learning/1.0/import-data/topics/ml-accessing-data-from-hdfs.html

# HDFS COMMANDS
!hdfs dfs -put README.md /tmp
!hdfs dfs -cat /tmp/README.md

# WORD COUNT WITH SPARK
from __future__ import print_function
import sys, re
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession\
  .builder\
  .appName("PythonWordCount")\
  .getOrCreate()

# read the file from HDFS, split into words, count number of occurances
lines = spark.read.text("/tmp/README.md").rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
  .map(lambda x: (x, 1)) \
  .reduceByKey(add) \
  .sortBy(lambda x: x[1], False)
output = counts.collect()
for (word, count) in output:
  print("%s: %i" % (word, count))

spark.stop()

# WORD COUNT WITH DASK
# https://distributed.dask.org/en/latest/examples/word-count.html