import java.text.SimpleDateFormat
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
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.state.WindowStore

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
      Thread.sleep(1000)
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
    kafkaConsumerProperties.put("auto.offset.reset", "earliest")

    kafkaConsumerProperties
  }


  //we set kafka stream properties
  def getkafkaStreamProperties() : Properties = {

    val  kafkfaStreamProperties : Properties= new Properties()
    kafkfaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe")
    kafkfaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkfaStreamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass())
    kafkfaStreamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass())
    kafkfaStreamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, (10 * 1024 * 1024L).toString) // Enable record cache of size 10 MB
    kafkfaStreamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000") // Set commit interval to 1 second.

    kafkfaStreamProperties
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

  //send data to kafka using just the topic name
  def sendDataToKafkaTopic(kafkaProducer: KafkaProducer[String, String], topic: String , key: String,  value: String): Unit = {

      try {
        val record = new ProducerRecord(topic, key, value)
        kafkaProducer.send(record)
    } catch{
        case e: Throwable => e.printStackTrace()
      }
  }

  //Here we read kafka partition
  def readKafkaPartition(topic:String, partitionNumber:Int) {

    val consumerPartition = new KafkaConsumer[String, String](getkafkaConsumerProperties("something"))

    //consumer.subscribe(util.Collections.singletonList(TOPIC))

    val partitionNum: TopicPartition  = new TopicPartition(topic, partitionNumber)
    //TopicPartition partition1 = new TopicPartition(TOPIC, 1)
    consumerPartition.assign(util.Collections.singletonList(partitionNum))

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

  //extract timestamp string
  def extractTimestampFromString(str: String): String = {
    val pattern = "(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}) .*".r

    str match {
      case pattern(timestamp) => timestamp
      case _ => ""
    }
  }

  //Read topic input  and feed topic output with the key = timestamp
  def readKafkaPartitionAndFeedTopic(topicInput:String, partitionNumber:Int, topicOutput: String) {

    val consumerPartition = new KafkaConsumer[String, String](getkafkaConsumerProperties("something"))
    val partitionNum: TopicPartition  = new TopicPartition(topicInput, partitionNumber)
    consumerPartition.assign(util.Collections.singletonList(partitionNum))

    val records = consumerPartition.poll(30000)  //during 30 seconds we retrieve data

    if (records.iterator().hasNext) {
      //We create the producer
      val producer = new KafkaProducer[String, String](getkafkaProducerProperties)

      for (rec <- records.asScala) {

        //timestamp extraction and conversion to mn
        val timeStampExtract = getTimestamp(extractTimestampFromString(rec.value()))

        timeStampExtract match {
          case Success(timeStampDate) => {
                                          val timeStampMn = timeStampDate/60000

                                          //we send data with the key equal the timestamp
                                          sendDataToKafkaTopic(producer, topicOutput, timeStampMn.toString, rec.value())
                                          println(s"Data sent to the topic '$topicOutput")
                                        }
          case Failure(st) => println(s"Failed to extract timestamp in the line : $st")

        }

      }
    } else {
      println(s"There is not data  for the topic '$topicInput' on the partition $partitionNumber")
    }

  }


  //This function return a topology for a kafka stream topic
  def getTopology(topicToCount : String) : Topology = {
    val builder = new StreamsBuilder()

    //we create our streaming source from the topic info
    val streamSource: KStream[String, String] = builder.stream(topicToCount)

    //we created a window store named "CountsWindowStore" that contains the counts for each key in 1-minute windows

    val countsError: KTable[Windowed[scala.Predef.String], java.lang.Long] = streamSource.groupByKey()
      .windowedBy(TimeWindows.of(60000))
      .count(Materialized.as[scala.Predef.String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("CountsWindowStore") withKeySerde (Serdes.String()))


    //we write the count result to another output and provide overridden serialization methods for Long
    //countsError.toStream().to(Windowed[String],Serdes.Long() ,"stream-grouped-output")

    //Here we glance on our topology
    val topology = builder.build()

    topology
  }

  //this function create a stream configuration for kafka
  def configureKafkaStream(topicToCount : String): KafkaStreams = {
    val kafkfaStreamProperties = getkafkaStreamProperties()
    val topology: Topology = getTopology(topicToCount)
    // we print the topology build
    println(topology.describe())

    //we can now build the Streams client
    val streams: KafkaStreams = new KafkaStreams(topology, kafkfaStreamProperties)
    val latch = new CountDownLatch(3)

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread(s"streams-shutdown-hook-$topicToCount") {
      override def run() {
        streams.close() //we close the stream
        latch.countDown() //we wait 5s until all workers have completed before closing
      }
    })

    streams
  }

}
