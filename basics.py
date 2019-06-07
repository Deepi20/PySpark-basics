from pyspark import SparkContext
from pyspark import SparkConf
sc = SparkContext("local",'test123')
sc
sc.stop()
sc = SparkContext(master='local[2]')
data = [12,32,45,65,67,89]
print data
rdd1 = sc.parallelize(data)
rdd1.collect()
rdd1.count()
rdd1.getNumPartitions()
sc.parallelize([]).isEmpty()
sc.parallelize([data]).isEmpty()
rdd1.max()
rdd1.min()
rdd1.stdev()
rdd1.mean()
rdd1.variance()
rdd1.stats()
import numpy as np
from pyspark.mllib.stat import Statistics
mat = sc.parallelize(
    [np.array([10.1,12.4,14.5,16.8,21]),np.array([21.3,24.2,35.4,36.4,31.7]),np.array([21.1,23.,54.,65.,71.])]
)
summary=Statistics.colStats(mat)
summary.mean()
summary.variance()
summary.numNonzeros()
X = sc.parallelize([10.1,12.4,14.5,16.8,21])
Y = sc.parallelize([21.3,24.2,35.4,36.4,31.7])
corr = Statistics.corr(X,Y,method='pearson')
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
vec = Vectors.dense(10.1,12.4,14.5,16.8,21,21.3,24.2,35.4,36.4,31.7)
goodnestest = Statistics.chiSqTest(vec)
print (goodnestest)
data = pd.read_csv("https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data",header=None, sep=',')
from pyspark.mllib.regression import LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD
sc.stop()
sc = SparkContext(appName='MLAlgo')
data = sc.textFile("C:\Users\Dell\Documents\winequality.csv") \
       .map(lambda line: line.split(",")) \
    .filter(lambda line: len(line)>1)\
    .map(lambda line: (line[0],line[3],line[2]))\
    .collect()
print (data)
parsed_data = [LabeledPoint(0.0,[14.23,1.71,2.43,15.6]),
              LabeledPoint(0.0,[13.2,1.78,2.14,11.2]),
              LabeledPoint(1.0,[21.3,32.4,3.5,21.4]),
              LabeledPoint(1.0,[12.4,21.4,21.7,32.8]),
              LabeledPoint(2.0,[21,65,45,21]),
              LabeledPoint(2.0,[21.5,76.8,54.6,54.9])]
from pyspark.mllib.classification import LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD
model = LogisticRegressionWithLBFGS.train(sc.parallelize(parsed_data),numClasses=3)
model.intercept
model.weights
