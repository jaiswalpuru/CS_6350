from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    #read
    business = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/business.csv")
    review = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/review.csv")
    user = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/user.csv")
    
    #preprocess
    business = business.map(
        lambda x: x.split('::')).map(lambda x:(x[0], tuple(x[1:])))
    review = review.map(
        lambda x: x.split('::')).map(lambda x: (str(x[2]), (str(x[1]), float(x[3]))))
    user = user.map(
        lambda x: x.split('::')).map(lambda x: (str(x[0]), str(x[1][:len(str(x[1]))-1])))
    
    # filter out data which only has stanford word in it
    stanford_business = business.filter(lambda x: 'Stanford' in x[1][0])
    
    _res = stanford_business.join(review).map(
        lambda x:x[1][1]).distinct().join(user).map(lambda x: (x[1][1],x[1][0]))
    
    res = _res.collect()
    
    sc.parallelize(res).saveAsTextFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/Q3_HW")