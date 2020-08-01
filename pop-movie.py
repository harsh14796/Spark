from pyspark import SparkConf, SparkContext
#import collections

conf = SparkConf().setMaster("local").setAppName("Popular_movie")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: int(x.split()[1]))
MovieCount = ratings.countByValue()
SortedResult = MovieCount.sortBy(lambda x: x[1])
#TotalMovieCount = ratings.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
#SortedResult = TotalMovieCount.map(lambda x: (x[1],x[0])).sortByKey(False)

result = SortedResult.collect()

#print(result[0][0] + ":\t\t" + result[0][1])
