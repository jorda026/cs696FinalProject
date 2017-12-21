import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer



class kafkaIO extends java.io.Serializable{

  def createTopicIntoKafka(topic: String, numPartitions: Int, replicationFactor: Int): Unit = {
    val zookeeperConnect = "localhost:2181"
    val sessionTimeoutMs = 10 * 1000
    val connectionTimeoutMs = 8 * 1000

    val zkClient = ZkUtils.createZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection = new ZkConnection(zookeeperConnect), isSecure = false)
    AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, new Properties)
    zkClient.close()
    print("Topic created: "+topic)
  }

  def printMessagesConsumer(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "consumer-fromtwitter")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(util.Arrays.asList("consumer-fromtwitter", "testtopic"))
    while ( {
      true
    }) {
      val records = kafkaConsumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {
        println("offset = "+record.offset+", key = "+record.key+", value = "+record.value+"\n\n")
      }
    }

    }


  def sendData(topic: String, message: String) = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //    props.put("partitioner.class", "com.fortysevendeg.biglog.SimplePartitioner")
    //    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")

    val kafkaProducer = new KafkaProducer(props, new StringSerializer(), new StringSerializer())
    kafkaProducer.send(new ProducerRecord[String, String](topic, message))
  }

  /* *********************** Create Kafka stuff  *********************** */

}
