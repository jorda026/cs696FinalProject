package com.fortysevendeg.log.utils

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer



object Kafka_func {

  /* *********************** Create Kafka Stuff *********************** */
  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("bootstrap.servers", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    //    props.put("partitioner.class", "com.fortysevendeg.biglog.SimplePartitioner")
    //    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")

    val config = new KafkaProducer(props, new StringSerializer(), new StringSerializer())
    return config


  }

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

  def createKafkaConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "consumer-fromtwitter")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    new KafkaConsumer[String, String](props)
  }

  def printMessagesConsumer(consumer: KafkaConsumer[String, String]): Unit = {

    consumer.subscribe(util.Arrays.asList("consumer-fromtwitter", "testtopic"))
    while ( {
      true
    }) {
      val records = consumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {
        println("offset = "+record.offset+", key = "+record.key+", value = "+record.value+"\n\n")
      }
    }

    }


  def sendData(kafkaProducer: KafkaProducer[String, String], topic: String, message: String) = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, message))
  }


  /* *********************** Create Kafka stuff  *********************** */

}
