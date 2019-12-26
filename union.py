#Simple word count
from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.textFile("union1.txt")
rdd2 = sc.textFile("union2.txt")
rdd3 = rdd1.map(lambda line : (line,line))
rdd4 = rdd2.map(lambda line : (line,line))
rdd5 = (rdd3+rdd4).reduceByKey(lambda v1,v2 : v1)
output = rdd5.collect()
for tpl in output:
    print(tpl[1])
