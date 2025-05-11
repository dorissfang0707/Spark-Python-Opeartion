from pyspark import SparkContext, SparkConf
#from operator import add
#import os
import sys
import json


#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html
conf = SparkConf()
conf.setMaster("local[*]").setAppName("hw1-task1").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
#Use SparkContext library and read file as text and then map it to json.
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

#set up the path & Read the data: 
InputJson = sys.argv[1] #test_review.json
OutputJson = sys.argv[2]
#Using textFile() method we can read a text (.txt) file into RDD.
dtRDD = sc.textFile(InputJson)
dtRDD = dtRDD.map(lambda x: json.loads(x))

#A. The total number of reviews (0.5 point)
total_reviews = dtRDD.map(lambda x: x['review_id']).count()

#B. The number of reviews in 2018 (0.5 point)
reviewsin2018 = dtRDD.filter(lambda x: '2018' in x['date']).count()

#C. The number of distinct users who wrote reviews (0.5 point)
dist_users = dtRDD.map(lambda x: x['user_id']).distinct().count()

#D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote (0.5 point)
#https://gab41.lab41.org/transformers-rdds-in-disguise-5890de0ef728
longest_reviews_0 = dtRDD.map(lambda x:(x['user_id'],1)).reduceByKey(lambda a,b: a+b)
longest_reviews = longest_reviews_0.takeOrdered(10, key = lambda x: [-x[1],x[0]]) #take top 10

#E. The number of distinct businesses that have been reviewed (0.5 point)
dist_business = dtRDD.map(lambda x: x['business_id']).distinct().count()

#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had (0.5 point)
#using takeOrdered() together:
top_business = dtRDD.map(lambda x:(x['business_id'],1)).reduceByKey(lambda a,b: a+b).takeOrdered(10, key = lambda x: [-x[1],x[0]])

#output format:
task1_output = {
			"n_review" : total_reviews,
			"n_review_2018": reviewsin2018,
			"n_user": dist_users,
			"top10_user": longest_reviews,
			"n_business": dist_business,
			"top10_business": top_business
}

with open(OutputJson, 'w') as outfile:
	json.dump(task1_output, outfile)

