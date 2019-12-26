#Count words starting with 'I'
from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.textFile("words2.txt")
searchTerm = input("Enter the word to be searched: ")
rdd2 = rdd1.filter(lambda line : (searchTerm in line))
rdd3 = rdd2.map(lambda line : (line,line))
output = rdd3.collect()
for tpl in output:
    print(tpl[0])
