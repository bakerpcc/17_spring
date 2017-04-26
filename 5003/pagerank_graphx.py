from graphframes import *
from pyspark.sql.functions import *

v=spark.read.csv('/Users/pc/spark-2.1.0-bin-hadoop2.7/vertices.csv',header=True,inferSchema=True)
e=spark.read.csv('/Users/pc/spark-2.1.0-bin-hadoop2.7/edges.csv',header=True,inferSchema=True)

g=GraphFrame(v,e)
# g.vertices.show()
# g.edges.show()
# g.inDegrees.show()
# g.outDegrees.show()

results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False).show()
