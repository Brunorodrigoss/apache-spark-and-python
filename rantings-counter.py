from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///home/local/FARFETCH/bruno.rodrigo/Estudos/apache-spark-and-python/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2]) # Store informatio at positon 3 the file above
result = ratings.countByValue() # Count each times appear on ratings
sortedResults = collections.OrderedDict(sorted(result.items())) # Sort dict results

for key, value in sortedResults.items(): # Print key and value (rating and quatity)
    print("%s %i" % (key, value))