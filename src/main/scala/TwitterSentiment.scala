import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger._
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

import Sentiment.Sentiment

object TwitterSentiment {
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)


  def maxLengthSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if !text.isEmpty => {
      sentiment(text)
      extractSentiment(text)
    }
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  def sentiment(input: String): List[(String, Sentiment)] = Option(input) match {
    case Some(text) if !text.isEmpty => {
      val sentiments: List[(String, Sentiment)] = extractSentiments(text)
      println(sentiments)
      sentiments
    }
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString,Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
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

  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("TwitterStreamTopics").setMaster("local[*]")

    if (args.length < 2) {
      System.err.println("Correct usage: Program_Name inputTopic outputTopic <consumer key> <consumer secret> <access token> <access token secret>")
      System.exit(1)
    }

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    val inTopic = args(0)
    val outTopic = args(1)
    val topic = "trump"

    if (args.length > 2) {
      val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.takeRight(args.length - 2)
      println("Twitter API Keys: ", consumerKey, consumerSecret, accessToken, accessTokenSecret)
      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

      //System.setProperty("twitter4j.oauth.consumerKey", "Qwg0x2SpwaV7Bt1blVItBGi97")
      //System.setProperty("twitter4j.oauth.consumerSecret", "LK469wvA0gQUi5yhu8UpAmMputC4j1k7HA07BQLYXEzdsXdSv7")
      //System.setProperty("twitter4j.oauth.accessToken", "287057610-Umum96jVUzxzdFzFuZj8XDjynXGq6knZIqODm4dX")
      //System.setProperty("twitter4j.oauth.accessTokenSecret", "0im6WSXTc68zm6tjWUWxbrVrWikqrsPDxynkaABMkyezQ")

    } else {
      // Configure Twitter credentials using twitter.txt
      setupTwitter()
    }

    val ssc = new StreamingContext(spark, Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None, Array(topic))

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText()).map(text => maxLengthSentiment(text))

    statuses.print
    // Blow out each word into a new DStream
    //val tweetwords = statuses.flatMap(tweetText => tweetText.split(" ").map(tweetText => tweetText.toLowerCase()))

    //// Map each tweetword to a key/value pair of (tweetword, 1) so we can count them up by adding up the values
    //val dataChatterKeyValues = tweetwords.map(tweetword => (tweetword, 1))
    //
    //// Now count them up over a 7 minute window sliding every two seconds
    //val dataChatterCounts = dataChatterKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(420), Seconds(2))
    //
    //// Sort the results by the count values
    //val sortedResults = dataChatterCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    //

    // Print the top 10
    //sortedResults.print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("../kafka/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    /*
      import spark.implicits._

      val lines = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", inTopic)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]

      val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().toDF("key", "value")

      val query_console = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()

      val query = wordCounts
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .outputMode("complete")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", outTopic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "src/main/kafkaUpdateSink/chkpoint")
        .start()

      query.awaitTermination()
      query_console.awaitTermination()
     */
  }
}
