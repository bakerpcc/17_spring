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

### connected component ###

from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()
sc.setCheckpointDir('User/pc/checkpoint')
result = g.connectedComponents()
result.select("id", "component").orderBy("component").show() 
"""
+---+---------+
| id|component|
+---+---------+
|  a|        0|
|  d|        0|
|  e|        0|
|  f|        0|
|  b|        0|
|  c|        0|
+---+---------+
means that they all belong to one component which id is 0
"""

# start

v=spark.read.csv('/Users/pc/PycharmProjects/5003/output/yelpNetwork_i.csv',header=True,inferSchema=True)
v.count() 
e=spark.read.csv('/Users/pc/PycharmProjects/5003/output/yelpNetwork_e.csv',header=True,inferSchema=True)
e.count() 
g=GraphFrame(v,e)
result = g.connectedComponents()

r=result.select("id", "component")
r.groupBy('component').count().orderBy('count',ascending=False).show()

#label=r.groupBy('component').count().orderBy('count',ascending=False).head()[0]

subset=result.filter('component=0')
subset_id=subset.select('id')
subset_edge=e.join(subset_id,e['dst']==subset['id'],'leftsemi').join(subset_id,e['src']==subset['id'],'leftsemi')

g_cc=GraphFrame(subset_id,subset_edge)
results_cc = g_cc.pageRank(resetProbability=0.01, maxIter=10)
results_cc.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False).show()
