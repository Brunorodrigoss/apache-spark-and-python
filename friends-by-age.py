from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile("file:///home/local/bruno.rodrigo/Estudos/apache-spark-and-python/ml-100k/fakefriends.csv")

rdd = lines.map(parse_line)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averageByAge.collect()
for result in results:
    print(result)

