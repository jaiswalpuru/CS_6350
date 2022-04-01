from pyspark import SparkContext, SparkConf

def get_common_friends(line):
    user = line[0].strip()
    friends = line[1]
    if user != '':
        pair_list = []
        for f in friends:
            f = f.strip()
            if f != '':
                if int(f) < int(user):
                    user_friend = (f + "," + user, set(friends))
                else:
                    user_friend = (user + "," + f, set(friends))
                pair_list.append(user_friend)
        return pair_list

if __name__ == "__main__":
    sc = SparkContext.getOrCreate();
    users = sc.textFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/mutual.txt").map(
        lambda x: x.split("\t")).filter(
            lambda x: len(x) == 2).map(
                lambda x: [x[0], x[1].split(",")])

    user_mutual_friends = users.flatMap(get_common_friends)
    
    mutual_friends = user_mutual_friends.reduceByKey(
        lambda x,y: len(x.intersection(y)))
    
    final = mutual_friends.map(
        lambda x: (x[1],x[0])).sortByKey(False)
    
    total_mutual_friend = final.map(lambda x : x[0]).sum()
    
    t = final.map(lambda x : 1).sum()
    avg = total_mutual_friend/t
    
    final.filter(
        lambda x: x[0] < avg).map(lambda x:"{0}\t{1}".format(x[1],x[0])).saveAsTextFile("/FileStore/shared_uploads/pxj200018@utdallas.edu/Q2_HW")