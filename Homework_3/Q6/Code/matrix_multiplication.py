from pyspark.mllib.linalg import Matrices, DenseMatrix
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import *

import numpy as np

# initiate the spark session
ss = SparkSession \
    .builder \
    .master('yarn') \
    .appName('matrix-multiplication') \
    .enableHiveSupport() \
    .getOrCreate()

sc = SparkContext.getOrCreate()

m1 = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/matrix1.txt").map(lambda x:x.split(' '))
m1 = m1.map(lambda x:x[:len(x)-1])
_m1 = m1.collect()
m1row,m1col = len(_m1), len(_m1[0])

for i in range(m1row):
    _m1[i] = IndexedRow(i, [i]+_m1[i])

mat1 = IndexedRowMatrix(sc.parallelize(_m1))
    
m2 = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/matrix2.txt").map(lambda x:x.split(' '))
m2 = m2.map(lambda x: x[:len(x)-1])
_m2 = m2.collect()

m2row, m2col= len(_m2), len(_m2[0])

temp_m2 = [1] + [0]*m2col

for i in range(m2row):
    temp_m2 += [0] + _m2[i]

mat2 = DenseMatrix(m2row+1, m2col+1, temp_m2,isTransposed=True)

matrix_mul= mat1.multiply(mat2)

rdd = matrix_mul.rows \
    .map(lambda ele: (ele.index, ele.vector.toArray().tolist()[1:]))


rdd.coalesce(1).saveAsTextFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/Q6_HW")