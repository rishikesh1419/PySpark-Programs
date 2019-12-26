#Simple word count
from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.textFile("max.txt")
rdd2 = rdd1.map(lambda line : (line.split()[0],int(line.split()[1])))
rdd3 = rdd2.reduceByKey(lambda v1,v2 : max([v1,v2]))
output = rdd3.collect()
for tpl in output:
    print(tpl[0]," - ",tpl[1])
