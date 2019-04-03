from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("CustomersAmount")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    cid    = int(fields[0])
    amount = float(fields[2])
    return (cid, amount)
    
lines   = sc.textFile("file:///SparkCourse/customer-orders.csv")
results = lines.map(parseLine).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0])).collect()

for result in results:
    print("Customer ID %i Spent %f" % (result[0], result[1]))