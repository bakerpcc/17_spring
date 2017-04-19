numOfIterations = 10

lines = spark.read.text("pagerank_data.txt")
# You can also test your program on the follow larger data set:
# lines = spark.read.text("wasb://cluster@msbd.blob.core.windows.net/data/dblp.in")

a = lines.select(split(lines[0],' '))
links = a.select(a[0][0].alias('src'), a[0][1].alias('dst'))
outdegrees = links.groupBy('src').count()
ranks = outdegrees.select('src', lit(1).alias('rank'))

for iteration in range(numOfIterations):
	ranks=links.join(outdegrees,'src').join(ranks,'src')\
				.select('src','dst',(col('rank')/col('count')).alias('contribs'))\
				.withColumnRenamed('dst','dst')\
				.groupBy('dst').sum('contribs')\
				.select(col('dst').alias('src'),(0.85*col('sum(contribs)')+0.15).alias('rank'))

ranks.orderBy(desc('rank')).show()


"""
pagerank_data.txt

1 2
1 3
2 3
3 4
4 1
2 1
"""
