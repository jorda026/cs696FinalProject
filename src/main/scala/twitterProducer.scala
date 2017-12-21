import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

import scala.util.parsing.json.JSONObject

object twitterProducer {
  def main(args: Array[String]) {
    System.setProperty("twitter4j.oauth.consumerKey", "CCZNPiKwLvb0pGtp8RRDVq7bQ")
    System.setProperty("twitter4j.oauth.consumerSecret", "h1GuGvRGdEbKH2hRrgjST9xTWrjlwLaVDrLYjK6e9v9FqKVWDk")
    System.setProperty("twitter4j.oauth.accessToken", "939239589079683072-Du8OOh9pFJBCBk6CCOgKvhrABrkzvKM")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "3Vl26RPEx1fK5LWu7S3auzqVJDVGu1C9ZBPmFY2VSdKDq")
    println(s"args: ${args.toList}")
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
      setAppName("spark-twitter-stream-example").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Let's create the Spark Context using the configuration we just created
    val sparkContext = new SparkContext(sparkConfiguration)

    // Now let's wrap the context in a streaming one, passing along the window size
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    val producer = new kafkaIO()

    val keywordsToSearch = Seq("#NFL", "#NHL", "#MLB", "#NBA")
    val tweets = TwitterUtils.createStream(streamingContext, None, keywordsToSearch)

    val kafkaTopic = args(0)

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