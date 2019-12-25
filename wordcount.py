from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.textFile("words.txt")
rdd2 = rdd1.flatMap(lambda line : line.split())
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda v1,v2 : v1+v2)
output = rdd4.collect()
for tpl in output:
    print(tpl[0],"-",tpl[1])
