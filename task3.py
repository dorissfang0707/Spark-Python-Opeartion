from pyspark import SparkContext, SparkConf
#from operator import add
#import os
import sys
import json
import time

#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkConf.html
conf = SparkConf()
conf.setMaster("local[*]").setAppName("hw1-task3").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
#Use SparkContext library and read file as text and then map it to json.
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

#set up the path & Read the data: 
InputJson_1 = sys.argv[1] #test_review.json
InputJson_2 = sys.argv[2] #business.json
OutputJson_1 = sys.argv[3] #output_filepath_question_a
OutputJson_2 = sys.argv[4] #output_filepath_question_b

#Using textFile() method we can read a text (.txt) file into RDD.
#before we use python or spark, we are going to use the textfile() method to make it into RDD file
#then map the business_id, stars, city out for us to use later

start_time = time.time()
dtRDD_1 = sc.textFile(InputJson_1)
dtRDD_1 = dtRDD_1.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['stars']))

dtRDD_2 = sc.textFile(InputJson_2)
dtRDD_2 = dtRDD_2.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'], x['city']))
end_time = time.time()
loading_time = end_time - start_time
#A. What are the average stars for each city? (1 point)

#checkt the following code & the output file

#B.compare the execution time of using two methods to print top 10 cities with highest average stars. 

#In python: 
start_time = time.time()
#https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth
dtJoined = dtRDD_1.leftOuterJoin(dtRDD_2).map(lambda x: (x[1][1],x[1][0]))
# Calculate the numerators (i.e. the SUMs,average)
dtJoined = dtJoined.groupByKey().mapValues(lambda x: sum(x) / len(x)).collect() #Divide each SUM by it's denominator
dtJoined_sorted = sorted(dtJoined, key=lambda li: (-li[1],li[0]))[:10]
end_time = time.time()
python_time = end_time - start_time

#In spark:
start_time = time.time()
#https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth
dtJoined = dtRDD_1.leftOuterJoin(dtRDD_2).map(lambda x: (x[1][1],x[1][0]))
# Calculate the numerators (i.e. the SUMs,average)
dtJoined = dtJoined.groupByKey().mapValues(lambda x: sum(x) / len(x)) #Divide each SUM by it's denominator
dtJoined = dtJoined.takeOrdered(10, key = lambda li: [-li[1],li[0]])
end_time = time.time()
Spark_time = end_time - start_time


#Output_filepath_question_a
#https://learnpython.com/blog/write-to-file-python/
import csv
headers = ["city","star"]
dtJoined_a = dtRDD_1.leftOuterJoin(dtRDD_2).map(lambda x: (x[1][1],x[1][0]))
dtJoined_a = dtJoined_a.groupByKey().mapValues(lambda x: sum(x) / len(x)).collect() #Divide each SUM by it's denominator
dtJoined_sorted_0 = sorted(dtJoined_a, key=lambda list: (-list[1],list[0]))

with open(OutputJson_1, 'w', encoding='UTF8', newline='') as json_file:
    # Create a writer object
    writer = csv.writer(json_file)
    # Write the header
    writer.writerow(headers)
    # Write the data/  # Add multiple rows of data
    writer.writerows(dtJoined_sorted_0)

#output format:
m1_time = python_time + loading_time
m2_time = Spark_time + loading_time
reasons = "m2's time shorter than m1's! In this case, m2 is using PySpark, and m1 is using Python. I believed the following as below can explain the reason why m2 runs faster than m1: first, PySpark earns fast processing, because its framework processes large amounts of data much quicker than other conventional framework; Second, PySpark owns excellent cache and disk persistence; Third, PySpark is known for its reduced latency due to its in-memory processing."

Output = {
	"m1": m1_time,
	"m2": m2_time,
	"reason": reasons
}

with open(OutputJson_2, 'w') as outfile:
	json.dump(Output, outfile)
