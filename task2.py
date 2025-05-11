from pyspark import SparkContext, SparkConf
#from operator import add
#import os
import sys
import json

#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html
conf = SparkConf()
conf.setMaster("local[*]").setAppName("hw1-task2").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
#Use SparkContext library and read file as text and then map it to json.
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

#set up the path & Read the data: 
InputJson = sys.argv[1] #test_review.json
OutputJson = sys.argv[2] 
n_partition = int(sys.argv[3]) #2 

#Using textFile() method we can read a text (.txt) file into RDD.
dtRDD = sc.textFile(InputJson)
dtRDD = dtRDD.map(lambda x: json.loads(x))

#show the number of partitions for the RDD used for Task 1 Question F & the number of items per partition.

#F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
dt = dtRDD.map(lambda x:(x['business_id'],1))

No_partition_D = dt.getNumPartitions()
terms_partition_D = dt.glom().map(len).collect()  # get length of each partition
#print(min(l), max(l), sum(l)/len(l), len(l))  # check if skewed
#https://stackoverflow.com/questions/28687149/how-to-get-the-number-of-elements-in-partition
#use a customized partition function to improve the performance of map and reduce tasks
#A time duration (for executing Task 1 Question F) comparison between the default partition and the customized partition

#get time of a program's execution:
import time
start_time = time.time()
top_business = dt.reduceByKey(lambda a,b: a+b).takeOrdered(10, key = lambda x: [-x[1],x[0]])
#print("--- %s seconds ---" % (time.time() - start_time))
end_time = time.time()
defaultPartition_time = end_time - start_time

#The customized partition:
def bus_partitioner(business):
    return hash(business)% n_partition

new_part = dt.partitionBy(n_partition, bus_partitioner)

No_partition_C = new_part.getNumPartitions()

terms_partition_C = new_part.glom().map(len).collect()  # get length of each partition
#print(min(l), max(l), sum(l)/len(l), len(l))  # check if skewed

start_time_D = time.time()
top_business = new_part.reduceByKey(lambda a,b: a+b).takeOrdered(10, key = lambda x: [-x[1],x[0]])
end_time_D = time.time()
CustomPartition_time = end_time_D - start_time_D

#output format:
Output = {
      "default":{
        "n_partition": No_partition_D,
        "n_items": terms_partition_D,
        "exe_time": defaultPartition_time
      },
      "customized":{
        "n_partition": No_partition_C,
        "n_items": terms_partition_C,
        "exe_time": CustomPartition_time
      }
}

with open(OutputJson, 'w') as outfile:
	json.dump(Output, outfile)

