import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.kafka.clients.producer._
import Control.using
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Failure, Success, Try}
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._


 object ZStringSe extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

object Utils {

  //this is a helper to create kafka topic
  def createTopic(zookeeperHosts: String = "127.0.0.1:2181" , topicName: String, numPartitions: Int, numReplication: Int = 1) {
    //we set zookeeper parameters
    val zkClient: ZkClient = new ZkClient(zookeeperHosts, 10000, 10000, ZStringSe)
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false)
    val topicConfiguration: Properties  = new Properties()


    if (!AdminUtils.topicExists(zkUtils, topicName)) {
      try {
        AdminUtils.createTopic(zkUtils, topicName, numPartitions, numReplication, topicConfiguration)
        println(s"Topic created. name: $topicName, partitions: $numPartitions, replFactor: $numReplication")
      } catch {
        case e:Throwable => e.printStackTrace //println(s"Topic exists. name: $topicName")
      }
    } else {
            println(s"Topic already exists. name: $topicName")
    }
  }

  //we set kafka Producer properties
  def getkafkaProducerProperties() : Properties = {
    val  kafkaProducerProperties = new Properties()

    kafkaProducerProperties.put("bootstrap.servers", "localhost:9092")
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProducerProperties
  }

  //we set kafka consumer properties
  def getkafkaConsumerProperties(groupId:String) : Properties = {
    val  kafkaConsumerProperties = new Properties()
    kafkaConsumerProperties.put("bootstrap.servers", "localhost:9092")

    kafkaConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConsumerProperties.put("group.id", groupId)

    kafkaConsumerProperties
  }


  //We read the file
  def readTextFileWithTry(filename: String): Try[List[String]] = {
    Try {
      val lines = using(io.Source.fromFile(filename)) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      lines
    }
  }

  //convert string to timestamp
  def getTimestamp(stringTimestamp: String): Try[Long] = {
    Try {
      val formatter = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
      val dateParsed = formatter.parse(stringTimestamp)

      dateParsed.getTime()
    }
  }

  //this function return a key and partition id based on a list otherwise it returns "nokey"
  def getKey(keylist: List[String] = List("info", "error", "warn"), log: String): Tuple2[String, Int] = {
    var keyused = ""

    for(key <- keylist){
      if(log.contains(key)) keyused = key
    }

    keyused match {
      case "info" => ("info", 1)
      case "warn" => ("warn", 2)
      case "error" => ("error",  3)
      case _ => ("nokey", 4)
    }
  }

  //send data to kafka
  def sendDataToKafka(kafkaProducer: KafkaProducer[String, String], topic: String , timestamp: String,  value: String): Unit = {
    val timeStampExtract = getTimestamp(timestamp)
    val infoKey = getKey(log = value)

    timeStampExtract match {
      case Success(timeStampDate) => {
        val record = new ProducerRecord(topic, infoKey._2, timeStampDate, infoKey._1, value)
        kafkaProducer.send(record)
      }
      case Failure(st) => println(s"Failed to extract timestamp in the line : $st")

    }
  }

  //Here we read kafka partition
  def readKafkaPartition(topic:String, partitionNumber:Int) {

    val consumerPartition = new KafkaConsumer[String, String](getkafkaConsumerProperties("something"))

    //consumer.subscribe(util.Collections.singletonList(TOPIC))

    val partition3: TopicPartition  = new TopicPartition(topic, partitionNumber)
    //TopicPartition partition1 = new TopicPartition(TOPIC, 1)
    consumerPartition.assign(util.Collections.singletonList(partition3))

    //we retrieve data from kafka
    while (true) {
      val records = consumerPartition.poll(1000)
      var time = 0L
      var counter = 1

      if (records.iterator().hasNext) {
        time = records.iterator().next().timestamp() / 60000
      }

      for (rec <- records.asScala) {

        if (time == rec.timestamp() / 60000) counter += 1
        else {
          println(s"key: ${rec.key}, timestamp: $time, total number of events: $counter")
          counter = 1
          time = rec.timestamp() / 60000
        }
      }

    }
  }

}
