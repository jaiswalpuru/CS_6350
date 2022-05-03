import math
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS

if __name__ == "__main__":	
	sc =  SparkContext.getOrCreate()
	rat = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/shortened.dat").map(
		lambda x:x.split("::"))
	
	rat = rat.map(
		lambda x:(x[0],x[1],x[2]))

	#split the data
	train_data, test_data = rat.randomSplit([6,4], seed=0)

	pred_test = test_data.map(
		lambda x: (x[0], x[1]))

	min_error = float('inf')

	seed, iterations, rank, lamda = 10, 20, [10,16], 0.1

	for rank in ranks:
   		model = ALS.train(train_data, rank, seed=seed, iterations=iterations, lambda_=lamda , nonnegative=False, blocks=-1)

   		predictions = model.predictAll(pred_test).map(
   			lambda x: (((x[0], x[1]), x[2])))
   		predicted_rates = test_data.map(
   			lambda x: (((int(x[0]), int(x[1])), float(x[2])))).join(predictions)
   		error = math.sqrt(predicted_rates.map(
            lambda x: (x[1][0] - x[1][1]) ** 2).mean())
   		if error < min_error :
   			min_error = error

	print(min_error)