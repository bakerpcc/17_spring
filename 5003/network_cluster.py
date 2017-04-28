data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distData.collect()

v=spark.read.csv('wasb://5003@network5003.blob.core.windows.net/sales.csv',header=True,inferSchema=True)
v.show()


%%configure
{ "conf": {"spark.jars.packages": "graphframes:graphframes:0.3.0-spark2.0-s_2.11" }}
sc.addPyFile('wasb://5003@network5003.blob.core.windows.net/graphframes-0.3.0-spark2.0-s_2.11.jar')
v=spark.read.csv('wasb://5003@network5003.blob.core.windows.net/v.csv',header=True,inferSchema=True)
e=spark.read.csv('wasb://5003@network5003.blob.core.windows.net/e.csv',header=True,inferSchema=True)
v.show()

from graphframes import *
from pyspark.sql.functions import *

g=GraphFrame(v,e)
