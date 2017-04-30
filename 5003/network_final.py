# ./bin/pyspark --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11

from graphframes import *
from pyspark.sql.functions import *

# step1: create graph according to yelp network data
v=spark.read.csv('/Users/pc/PycharmProjects/5003/output/yelpNetwork_i.csv',header=True,inferSchema=True)
# v.count() 
e=spark.read.csv('/Users/pc/PycharmProjects/5003/output/yelpNetwork_e.csv',header=True,inferSchema=True)
# e.count() 
g=GraphFrame(v,e)

# step2: we need to make sure that this graph is a directed graph
# then we can run pagerank algorithm on it
a=g.inDegrees
# b=g.outDegrees.withColumnRenamed('id','out_id')
b=g.outDegrees
# inOut=a.join(b,a['id']==b['out_id'])
inOut=a.join(b,'id')
static=inOut.select('*',(inOut['inDegree']/inOut['outDegree']).alias('ratio')).select('id','ratio')
bio_ratio=float(static.filter("ratio=1").count())/float(g.vertices.count())

# step3: detect connected component
sc.setCheckpointDir('User/pc/checkpoint')
result = g.connectedComponents()
r=result.select("id", "component")
r.groupBy('component').count().orderBy('count',ascending=False).show()

# step4: choose the largest connected component, create a new subset graph, and run pagerank algorithm on this new graph
subset_0=result.filter('component=0')
subset_id=subset_0.select('id')
subset_edge=e.join(subset_id,e['dst']==subset_0['id'],'leftsemi').join(subset_id,e['src']==subset_0['id'],'leftsemi')

g_cc=GraphFrame(subset_id,subset_edge)
pr = g_cc.pageRank(resetProbability=0.01, maxIter=10)
pr.vertices.select("id", "pagerank").orderBy("pagerank",ascending=False).show()

# step5: we want to get the max pagerank vertices for each business, so we need (business_id,user_id) pair, extracted from review
review=spark.read.csv('/Users/pc/PycharmProjects/5003/output/yelpNetwork_b_u.csv',header=True,inferSchema=True)

# but if the number of one business's comment is too small, it will be meaningless for them to distribute coupons according 
# to this network's results, for they do not have enough data and do not have enough user to expand influence in cascanding.
# so we first groupBy business id and extract subset of business whose users' number is more than 100
# we consider these business is meaningful to use max pagerank user to express their coupons or make advertisement influence
# on new dishes or event

# in order to avoid spark bug on groupBy, we add withColumnRenamed before every groupBy operation
cnt=review.withColumnRenamed('business_id','business_id').groupBy('business_id').count().filter('count>100')
subset=cnt.join(review,'business_id')
pr_results_business=pr.join(subset,pr['id']==subset['user_id']).select("user_id","pagerank","business_id") /
                    .withColumnRenamed('business_id','business_id').groupBy('business_id').max()
  
# step5: write result into csv file. 
# For default setting, spark will write it into multi-csvfile distributely, we need to merge them into one csv file.
from subprocess import call
pr_results_business.write.format('com.databricks.spark.csv').save('5003/result')
os.system("cat 5003/result/p* > 5003/result.csv")

