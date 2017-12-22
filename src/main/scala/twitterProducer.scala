import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

import scala.util.parsing.json.JSONObject

object twitterProducer {
  def main(args: Array[String]) {
    // Set up twitter API keys to allow Spark-Scala to connect to twitter. Normally, these would be in a configuration
    // file hidden away, but we left them in for simplicity
    System.setProperty("twitter4j.oauth.consumerKey", "CCZNPiKwLvb0pGtp8RRDVq7bQ")
    System.setProperty("twitter4j.oauth.consumerSecret", "h1GuGvRGdEbKH2hRrgjST9xTWrjlwLaVDrLYjK6e9v9FqKVWDk")
    System.setProperty("twitter4j.oauth.accessToken", "939239589079683072-Du8OOh9pFJBCBk6CCOgKvhrABrkzvKM")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "3Vl26RPEx1fK5LWu7S3auzqVJDVGu1C9ZBPmFY2VSdKDq")
    // Print out the arguments passed into main
    println(s"args: ${args.toList}")
    //This function takes a twitter4j status and puts it into a JSON formatted object to put onto Kafka
    def tweetToMap(tweet: Status): Map[String, Object] = {
      val user = tweet.getUser()
      val locationMap = Option(tweet.getGeoLocation()) match {
        case Some(location) => {
          Map[String, Object](
            "lat" -> location.getLatitude().toString(),
            "lon" -> location.getLongitude().toString()
          )
        }
        case None => {
          Map[String, Object](
            "lat" -> "0",
            "lon" -> "0"
          )
        }
      }
      val userMap = Map[String, Object](
        "id" -> user.getId().toString(),
        "name" -> user.getName(),
        "screen_name" -> user.getScreenName(),
        "profile_image_url" -> user.getProfileImageURL()
      )
      return Map[String, Object](
        "user" -> new JSONObject(userMap),
        "location" -> new JSONObject(locationMap),
        "id" -> tweet.getId().toString(),
        "created_at" -> tweet.getCreatedAt().toString(),
        "text" -> tweet.getText()
      )
    }

    val sparkConfiguration = new SparkConf().
      setAppName("cs696FinalProject").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Let's create the Spark Context using the configuration we just created
    val sparkContext = new SparkContext(sparkConfiguration)

    // Now let's wrap the context in a streaming one, passing along the window size
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    // Create the Kafka input/output object
    val producer = new kafkaIO()

    val keywordsToSearch = Seq("#NFL", "#NHL", "#MLB", "#NBA")
    // Creates the Stream that will search twitter grabbing tweets that contain the values set above in keywordsToSearch
    val tweets = TwitterUtils.createStream(streamingContext, None, keywordsToSearch)

    val kafkaTopic = args(0)
    // Since the results are stored in RDDs, we need to iterate through each and every RDD to get to each individual
    // twitter4j status that actually contains the singular twitter data. There was an issue that if you did not run
    // an output operation to trigger the execution of the stream transformations you would get an error of:
    // "java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute"
    // Explanation can be found here: https://stackoverflow.com/questions/37612401/streaming-streamingcontext-error-starting-the-context-marking-it-as-stopped-s
    tweets.foreachRDD(tweetsRDD => {
      tweetsRDD.foreachPartition(tweetsRDDPartition => {
        tweetsRDDPartition.foreach(tweet => {
          println(tweet.getText())
          producer.sendData(kafkaTopic, (new JSONObject(tweetToMap(tweet))).toString())
        })
      })
    })
    streamingContext.start()
    val seconds = args(1).toInt
    val streamDuration = seconds * 1000
    // Let's await the stream to end - streamDuration
    streamingContext.awaitTerminationOrTimeout(streamDuration)
    // Once the timeout has been reached, stop the stream and the Spark context
    StreamingContext.getActive.foreach {
      _.stop(true, true)
    }

  }
}