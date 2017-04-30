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

"""
+---------+------+                                                              
|component| count|
+---------+------+
|        0|429208|
|    95949|     7|
|    84759|     7|
|    96933|     7|
|    49211|     6|
|    98354|     6|
|   276395|     5|
|   107731|     5|
|   204385|     5|
|   197665|     5|
|   137185|     5|
|   263313|     5|
|    94674|     5|
|     8825|     5|
|    80657|     5|
|   147090|     5|
|   171371|     5|
|    80086|     5|
|   210002|     5|
|   226268|     5|
+---------+------+
only showing top 20 rows
"""
#label=r.groupBy('component').count().orderBy('count',ascending=False).head()[0]

subset=result.filter('component=0')
subset_id=subset.select('id')
subset_edge=e.join(subset_id,e['dst']==subset['id'],'leftsemi').join(subset_id,e['src']==subset['id'],'leftsemi')

g_cc=GraphFrame(subset_id,subset_edge)
# results_cc = g_cc.pageRank(resetProbability=0.15, maxIter=3)
results_cc = g_cc.pageRank(resetProbability=0.01, maxIter=10)
results_cc.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False).show()
"""
+--------------------+------------------+                                       
|                  id|          pagerank|
+--------------------+------------------+
|PhUqhfyk3jdaS0Xb6...| 108.7074915473339|
|O_GWZZfQx7qv-n-CN...| 96.00746145078757|
|GGTF7hnQi6D5W77_q...| 90.81101504216208|
|NfU0zDaTMEQ4-X9db...| 89.74133466829397|
|qVc8ODYU5SZjKXVBg...|  87.4021831810235|
|8DEyKVyplnOcSKx39...| 85.02814512364793|
|-xDW3gYiYaoeVASXy...|  84.4364846550652|
|Wc5L6iuvSNF5WGBlq...| 75.77613487971219|
|iLjMdZi0Tm7DQxX1C...| 73.56168058066208|
|WeVkkF5L39888IPPl...| 67.53970492721238|
|dIIKEfOgo0KqUfGQv...| 65.82670355243518|
|4wp4XI9AxKNqJima-...| 60.62139086152308|
|jJDEwznWHQIaT4Z0l...| 59.89461065985523|
|qewG3X2O4X6JKskxy...| 57.22167924976696|
|AvC5XQAElcGAAn_Wr...|56.304683175152746|
|Q9mA60HnY87C1TW5k...| 51.84725590262247|
|cBFgmOCBdhYa0xoFE...|50.855597970411836|
|IU86PZPgTDCFwJEuA...| 49.95370284743756|
|djxnI8Ux8ZYQJhiOQ...|  48.2637695944795|
|wd3xoNaDLib8dhQ7B...| 45.80447922309248|
+--------------------+------------------+
"""

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

# 4,5
# NfU0zDaTMEQ4-X9dbQWd9A 96554
# qVc8ODYU5SZjKXVBgXdI7w 62416

total=g.vertices.count()
# result:1029432


# 分组选出最大的pagerank者

"""
review:
+-----------+-------+
|business_id|user_id|
+-----------+-------+
|         b1|      1|
|         b1|      2|
|         b1|      5|
|         b2|      3|
|         b2|      5|
|         b3|      2|
|         b3|      4|
|         b3|      4|
+-----------+-------+
r:
+---+--------------------+
| id|            pagerank|
+---+--------------------+
|  5|       0.04905846505|
|  4|         0.039452995|
|  3|           0.0297505|
|  2|0.014950000000000001|
|  1|                0.01|
+---+--------------------+

rr:
+---+--------------------+-----------+-------+
| id|            pagerank|business_id|user_id|
+---+--------------------+-----------+-------+
|  5|       0.04905846505|         b2|      5|
|  5|       0.04905846505|         b1|      5|
|  4|         0.039452995|         b3|      4|
|  4|         0.039452995|         b3|      4|
|  3|           0.0297505|         b2|      3|
|  2|0.014950000000000001|         b3|      2|
|  2|0.014950000000000001|         b1|      2|
|  1|                0.01|         b1|      1|
+---+--------------------+-----------+-------+

max:
+-----------+-------+-------------+------------+
|business_id|max(id)|max(pagerank)|max(user_id)|
+-----------+-------+-------------+------------+
|         b2|      5|0.04905846505|           5|
|         b3|      4|  0.039452995|           4|
|         b1|      5|0.04905846505|           5|
+-----------+-------+-------------+------------+
"""

v=spark.read.csv('/Users/pc/ziptest/v.csv',header=True,inferSchema=True)
e=spark.read.csv('/Users/pc/ziptest/e.csv',header=True,inferSchema=True)
review=spark.read.csv('/Users/pc/ziptest/review.csv',header=True,inferSchema=True)
g=GraphFrame(v,e)
results = g.pageRank(resetProbability=0.01, maxIter=10)
r=results.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False)

rr=r.join(review,r['id']==review['user_id'])
rr.groupBy('business_id').max().show()
business_result=rr.groupBy('business_id').max().select('business_id','max(user_id)')
business_result.show()



a=g.inDegrees
b=g.outDegrees.withColumnRenamed('id','out_id')
inOut=a.join(b,a['id']==b['out_id'])
static=inOut.select('*',(inOut['inDegree']/inOut['outDegree']).alias('ratio')).select('id','ratio')
bio_ratio=float(static.filter("ratio=1").count())/float(g.vertices.count())
