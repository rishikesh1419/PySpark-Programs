from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
from math import sqrt
from numpy import array

def error(point) :
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point-center)]))

sc = SparkContext()

data = sc.textFile("clust.txt")
header = data.first()
header = sc.parallelize([header])
data = data.subtract(header)
parseData = data.map(lambda line: array([float(x) for x in line.split(',')])).cache()
clusters = KMeans.train(parseData, 2, maxIterations=15, runs=10, initializationMode='random')
WSSSE = parseData.map(lambda point: error(point)).reduce(lambda x,y : x+y)
print("Within set sum of squared error: ",WSSSE)
print("Cluster centers are:")
for i in clusters.centers :
    print(i[0],",",i[1])
