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
sc.setCheckpointDir('User/pc/checkpoint')
result = g.connectedComponents()

r=result.select("id", "component")
r.groupBy('component').count().orderBy('count',ascending=False).show()

#label=r.groupBy('component').count().orderBy('count',ascending=False).head()[0]

subset=result.filter('component=0')
subset_id=subset.select('id')
subset_edge=e.join(subset_id,e['dst']==subset['id'],'leftsemi').join(subset_id,e['src']==subset['id'],'leftsemi')

g_cc=GraphFrame(subset_id,subset_edge)
# results_cc = g_cc.pageRank(resetProbability=0.15, maxIter=3)
results_cc = g_cc.pageRank(resetProbability=0.01, maxIter=10)
results_cc.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False).show()

temp=results_cc.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False)
temp.take(5)

"""
results:
PhUqhfyk3jdaS0Xb619RJQ,pagerank=108.7074915473339
O_GWZZfQx7qv-n-CN7hsIA,pagerank=96.00746145078757
GGTF7hnQi6D5W77_qiKlqg,pagerank=90.81101504216208
NfU0zDaTMEQ4-X9dbQWd9A,pagerank=89.74133466829397
qVc8ODYU5SZjKXVBgXdI7w,pagerank=87.4021831810235
"""

temp.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")

# 2-hop friend
# g_cc.find("(a)-[]->(b);(b)-[]->(c)").filter("a.id = 'PhUqhfyk3jdaS0Xb619RJQ'").count()
# result:504781 with redundancy

top1=g_cc.find("(a)-[]->(b);(b)-[]->(c)").filter("a.id = 'PhUqhfyk3jdaS0Xb619RJQ'").select("c.id").distinct().count()
# result:504781 with redundancy
# result:87051
top2=g_cc.find("(a)-[]->(b);(b)-[]->(c)").filter("a.id = 'O_GWZZfQx7qv-n-CN7hsIA'").select("c.id").distinct().count()
# result:333003 with redundancy
# result:77509
top3=g_cc.find("(a)-[]->(b);(b)-[]->(c)").filter("a.id = 'GGTF7hnQi6D5W77_qiKlqg'").select("c.id").distinct().count()
# result:130071 with redundancy
# result:39505

# randomly pick a vertices and compare it's 2-hop friend degree wuth high pagerank vertices
random=g_cc.find("(a)-[]->(b);(b)-[]->(c)").filter("a.id = 'tDfedUfC7n33WBQtkPD5aw'").select("c.id").distinct().count()
# result:401 with redundancy
# result:332 

total=g.vertices.count()
# result:1029432
