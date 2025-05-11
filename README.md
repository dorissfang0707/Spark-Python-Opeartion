#### Spark RDD opearion 

1. The data is a real-world dataset: Yelp dataset which I download from https://www.yelp.com/dataset.
   1. test_review.json, which contains the review information from users
2. Programming Environment: Python 3.6, JDK 1.8, Scala 2.12, and Spark 3.1.2

#### For Task1, I am working on the following questions:

    A. The total number of reviews
  
    B. The number of reviews in 2018
  
    C. The number of distinct users who wrote reviews
  
    D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
  
    E. The number of distinct businesses that have been reviewed
  
    F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had

#### For Task2, I am trying to show the number of partitions for the RDD used for Task 1 Question F and the number of items per partition. Then I used a customized partition function to improve the performance of map and reduce tasks. In the end, I created a time duration (for executing Task 1 Question F) comparison between the default partition and the customized partition.

#### For Task3, explore two datasets together containing review information (test_review.json) and business information (business.json) and write a program to answer the following questions:

    A. What are the average stars for each city
  
    B. Compare the execution time of using two methods to print top 10 cities with highest average stars. 

