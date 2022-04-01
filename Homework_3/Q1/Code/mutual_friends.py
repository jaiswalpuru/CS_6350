from pyspark import SparkContext

def get_possible_pairs(line):
    user_id = line[0]
    friend = line[1].split(",")
    pair_list = []
    for f in friend:
        if f != '' and f != user_id:
            if int(user_id) < int(f):
                pair = (user_id + "," + f, set(friend))
            else:
                pair = (f + "," + user_id, set(friend))
            pair_list.append(pair)
    return pair_list

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    
    lines = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/mutual.txt")
    
    users = lines.map(
        lambda line: line.split("\t")).filter(
            lambda line: len(line) == 2).flatMap(get_possible_pairs)
    
    mutual_friends = users.reduceByKey(
        lambda x, y: x.intersection(y))
    
    mutual_friends.map(
        lambda x:"{0}\t{1}".format(x[0],len(x[1]))).sortBy(
            lambda x: x.split("\t")[0]).sortBy(
                lambda x: x.split("\t")[1]).saveAsTextFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/Q1_HW")