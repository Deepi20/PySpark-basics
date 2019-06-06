from pyspark import SparkContext
from pyspark import SparkConf
sc = SparkContext("local",'test123')
sc
sc.stop()
sc = SparkContext(master='local[2]')
data = [12,32,45,65,67,89]
