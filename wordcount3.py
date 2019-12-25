#Count 4-letter words
from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.textFile("words.txt")
rdd2 = rdd1.flatMap(lambda line : line.split())
rdd3 = rdd2.filter(lambda word : len(word)==4)
rdd4 = rdd3.map(lambda word : (word,1))
rdd5 = rdd4.reduceByKey(lambda v1,v2 : v1+v2)
output = rdd5.collect()
for tpl in output:
    print(tpl[0],"-",tpl[1])
