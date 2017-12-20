//import org.apache.spark._
//import org.apache.spark.storage._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.twitter.TwitterUtils
//
//import scala.math.Ordering
//
//import twitter4j.auth.OAuthAuthorization
//import twitter4j.conf.ConfigurationBuilder
//import twitter4j.Status

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import com.fortysevendeg.log.utils.Kafka_func._

object twitterStreamer extends App {
  System.setProperty("twitter4j.oauth.consumerKey", "CCZNPiKwLvb0pGtp8RRDVq7bQ")
  System.setProperty("twitter4j.oauth.consumerSecret", "h1GuGvRGdEbKH2hRrgjST9xTWrjlwLaVDrLYjK6e9v9FqKVWDk")
  System.setProperty("twitter4j.oauth.accessToken", "939239589079683072-Du8OOh9pFJBCBk6CCOgKvhrABrkzvKM")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "3Vl26RPEx1fK5LWu7S3auzqVJDVGu1C9ZBPmFY2VSdKDq")

  println("Hello")

  // Directory to output top hashtags
//  val outputDirectory = "/twitter"
//
//  // Recompute the top hashtags every 1 second
//  val slideInterval = new Duration(1 * 1000)
//
//  // Compute the top hashtags for the last 5 seconds
//  val windowLength = new Duration(5 * 1000)
//
//  // Wait this many seconds before stopping the streaming job
//  val timeoutJobLength = 100 * 1000
//
//  var newContextCreated = false
//  var num = 0
//
//  // This is a helper class used for
//  object SecondValueOrdering extends Ordering[(String, Int)] {
//    def compare(a: (String, Int), b: (String, Int)) = {
//      a._2 compare b._2
//    }
//  }
//
//  // This is the function that creates the SteamingContext and sets up the Spark Streaming job.
//  def creatingFunc(): StreamingContext = {
//    // Create a Spark Streaming Context.
//    val conf = new SparkConf().setAppName("Assignment 5").setMaster("local[*]").set("spark.sql.session.timeZone", "PST")
//
//    val sc = new SparkContext(conf)
//
//    val ssc = new StreamingContext(sc, slideInterval)
//    // Create a Twitter Stream for the input source.
//    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
//    val twitterStream = TwitterUtils.createStream(ssc, auth)
//
//    // Parse the tweets and gather the hashTags.
//    val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
//
//    // Compute the counts of each hashtag by window.
//    val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)
//
//    // For each window, calculate the top hashtags for that time period.
//    windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
//      val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
//      println(s"------ TOP HASHTAGS For window ${num}")
//      println(topEndpoints.mkString("\n"))
//      num = num + 1
//    })
//
//    newContextCreated = true
//    ssc
//}
//
//  val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
//  ssc.start()
//  ssc.awaitTerminationOrTimeout(timeoutJobLength)
  // You can find all functions used to process the stream in the
  // Utils.scala source file, whose contents we import here
  import Utils._

  // First, let's configure Spark
  // We have to at least set an application name and master
  // If no master is given as part of the configuration we
  // will set it to be a local deployment running an
  // executor per thread
/* *********************** Kafka Part *********************** */
  val producer = createKafkaProducer()

  d(producer,"testtopic", "Testing")
  val cons = createKafkaConsumer()

  printMessagesConsumer(cons)
  
  
  /* *********************** Kafka Part *********************** */


  val sparkConfiguration = new SparkConf().
    setAppName("spark-twitter-stream-example").
    setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

  // Let's create the Spark Context using the configuration we just created
  val sparkContext = new SparkContext(sparkConfiguration)

  // Now let's wrap the context in a streaming one, passing along the window size
  val streamingContext = new StreamingContext(sparkContext, Seconds(5))

  // Creating a stream from Twitter (see the README to learn how to
  // provide a configuration to make this work - you'll basically
  // need a set of Twitter API keys)
  val tweets = TwitterUtils.createStream(streamingContext, None)

  val statuses = tweets.map(status => if(status.getLang() == "en") status.getText() else status.getLang())
  statuses.print(255)
//  // To compute the sentiment of a tweet we'll use different set of words used to
//  // filter and score each word of a sentence. Since these lists are pretty small
//  // it can be worthwhile to broadcast those across the cluster so that every
//  // executor can access them locally
//  val uselessWords = sparkContext.broadcast(load("/stop-words.dat"))
//  val positiveWords = sparkContext.broadcast(load("/pos-words.dat"))
//  val negativeWords = sparkContext.broadcast(load("/neg-words.dat"))
//
//  // Let's extract the words of each tweet
//  // We'll carry the tweet along in order to print it in the end
//  val textAndSentences: DStream[(TweetText, Sentence)] =
//  tweets.
//    map(_.getText).
//    map(tweetText => (tweetText, wordsOf(tweetText)))
//
//  // Apply several transformations that allow us to keep just meaningful sentences
//  val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
//    textAndSentences.
//      mapValues(toLowercase).
//      mapValues(keepActualWords).
//      mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
//      filter { case (_, sentence) => sentence.length > 0 }
//
//  // Compute the score of each sentence and keep only the non-neutral ones
//  val textAndNonNeutralScore: DStream[(TweetText, Int)] =
//    textAndMeaningfulSentences.
//      mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
//      filter { case (_, score) => score != 0 }
//
//  // Transform the (tweet, score) pair into a readable string and print it
//  textAndNonNeutralScore.map(makeReadable).print

//  // Now that the streaming is defined, start it
//  streamingContext.start()
//
//  // Let's await the stream to end - forever
//  streamingContext.awaitTermination()

}
