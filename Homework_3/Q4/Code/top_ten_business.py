from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    #read
    business = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/business.csv")
    review = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/review.csv")
    
    #preprocess
    business = business.map(
        lambda x: x.split('::')).map(lambda x:(x[0], tuple(x[1:])))
    review = review.map(
        lambda x: x.split('::')).map(lambda x: (str(x[2]), (1)))
    
    sorted_rating_sum = review.reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1], False)
    result = sorted_rating_sum.join(business).distinct().map(
        lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][0])).sortBy(
        lambda x: x[3],False).collect()[:10]
    
    final_result = sc.parallelize(result)
    final_result.coalesce(1).saveAsTextFile('/FileStore/shared_uploads/pxj200018@utdallas.edu/Q4_HW')