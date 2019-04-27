# twitter-sentiment
Spark Streaming with Twitter and Kafka

## Assets
`./data/AirlineTweets.csv`
 

## Required config files
```bash
./logstash_kafka.conf

``` 

## Installations
```bash
elasticsearch
kibana
logstash - Run logstash with the above config file
kafka
```

## Build Steps
```jshelllanguage
sbt

sbt>assembly
```

Above steps should create a jar file under target folder
 
## PART 1 - Steps to run TwitterSentiment class

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class TwitterSentiment ./target/scala-2.11/kafka-assembly-0.1.jar <kafkaTopic> <consumer key> <consumer secret> <access token> <access token secret>

(or)

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class TwitterSentiment ./target/scala-2.11/kafka-assembly-0.1.jar <kafkaTopic> 

eg: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class TwitterSentiment ./target/scala-2.11/kafka-assembly-0.1.jar topicA

```


## Images
Part 1 - 
Kibana dashboard screenshot

## Results 
Complete integration is done and also used visualation tools in kibana  

## PART 2 - Steps to run AirlineSentiment class
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class AirlineSentiment ./target/scala-2.11/kafka-assembly-0.1.jar <inputPath> <outputPath>

eg: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class AirlineSentiment ./target/scala-2.11/kafka-assembly-0.1.jar data/AirlineTweets.csv output.txt
```


1) Create an S3 bucket in AWS.
2) Upload generated jar (kafka-assembly-0.1.jar) and AirlineTweets.csv. 
3) Start an AWS EMR cluster and select Spark Application by clicking Add step 
4) For running AirlineSentiment class, enter "--class "AirlineSentiment"" in spark-submit options and choose the jar uploaded in s3://<your_s3_bucket>/ and enter the arguments as below:

  Argument 1 = Location of dataset (csv). In this case,
  s3://<s3_bucket>/AirlineTweets.csv

  Argument 2 = Location to create output file.
  S3://s3_bucket/

Once done check for the output file for classification metrics. 

I'm attaching my output file that I ran in local. 

## Images
Part 2 - 
Classification output

## Results
```text
Tested Logisitic Regression and Naye Bayes and chose Logistic Regresion as its giving more accuracy 
```

