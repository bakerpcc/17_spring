import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/pc/spark-2.1.0-bin-hadoop2.7"
os.environ["PYSPARK_PYTHON"]="/Users/pc/.pyenv/versions/2.7.9/bin/python"
# You might need to enter your local IP
# os.environ['SPARK_LOCAL_IP']="192.168.2.138"

# Path for pyspark and py4j
sys.path.append("/Users/pc/spark-2.1.0-bin-hadoop2.7/python")
sys.path.append("/Users/pc/spark-2.1.0-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

sc = SparkContext('local')

x = [1, 4, 3, 5, 6, 7, 0, 1]

rdd = sc.parallelize(x, 4).cache()

def f(iterator):
    s = 0
    for i in iterator:
        s += i
    yield s

sums = rdd.mapPartitions(f).collect()

for i in range(1, len(sums)):
    sums[i] += sums[i-1]

def g(index, iterator):
    global sums
    if index == 0:
        s = 0
    else:
        s = sums[index-1]
    for i in iterator:
        s += i
        yield s

prefix_sums = rdd.mapPartitionsWithIndex(g)
print prefix_sums.collect()
