import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.TwitterResponse
import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import Sentiment.Sentiment

object TwitterSentiment {
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)


  def maxLengthSentiment(input: String): (String, Sentiment) = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  def sentiment(input: String): List[(String, Sentiment)] = Option(input) match {
    case Some(text) if !text.isEmpty => {
      val sentiments: List[(String, Sentiment)] = extractSentiments(text)
      //println(sentiments)
      sentiments
    }
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): (String, Sentiment) = {
    val (sentence, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    (sentence, sentiment)
  }

  def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }


  def checkAndWait(response: TwitterResponse, verbose: Boolean = false) {
    val rateLimitStatus = response.getRateLimitStatus
    if (verbose) println("RLS: " + rateLimitStatus)

    if (rateLimitStatus != null && rateLimitStatus.getRemaining == 0) {
      println("*** You hit your rate limit. ***")
      val waitTime = rateLimitStatus.getSecondsUntilReset + 10
      println("Waiting " + waitTime + " seconds ( " + waitTime/60.0 + " minutes) for rate limit reset.")
      Thread.sleep(waitTime*1000)
    }
  }

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    println("Twitter API Keys: ")
    for (line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        println(fields(0), fields(1))
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  def kafkaConf(topic: String) = {
    val serializer = "org.apache.kafka.common.serialization.StringSerializer"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", serializer)
    props.put("value.serializer", serializer)
    props
  }


  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("TwitterStreamTopics").setMaster("local[*]")

    if (args.length < 1) {
      System.err.println("Correct usage: Program_Name inputTopic outputTopic <consumer key> <consumer secret> <access token> <access token secret>")
      System.exit(1)
    }

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val kafkaTopic = args(0)
    val filter = "trump"

    if (args.length > 1) {
      val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.takeRight(args.length - 2)
      println("Twitter API Keys: ", consumerKey, consumerSecret, accessToken, accessTokenSecret)
      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    } else {
      // Configure Twitter credentials using twitter.txt
      setupTwitter()
    }

    val ssc = new StreamingContext(spark, Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None, Array(filter))

    // Now extract the text of each status update into DStreams using foreachRDD()
    tweets.foreachRDD(rdd => {
      rdd.cache()
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaTopic))
      rdd.collect().toList.foreach(tweet => {
        val txt = tweet.getText()
        val (sentence, sentiment) = maxLengthSentiment(txt)
        //print(sentence, sentiment)
        producer.send(new ProducerRecord[String, String](kafkaTopic, sentence, sentiment.toString()))
      })
      rdd.unpersist()
    })

    // Set checkpoint directory for continous streaming to work
    ssc.checkpoint("../kafka/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
