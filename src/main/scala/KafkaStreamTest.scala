import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import Producer.TOPIC
import Utils._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.StreamPartitioner
//import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreTypes, ReadOnlyWindowStore, WindowStore}
//import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KeyValueMapper

object KafkaStreamTest extends App {



  println("**************** Step 1: Configuration ************")

  val kafkfaStreamProperties = getkafkaStreamProperties()
  val  builder = new StreamsBuilder()
  val topicToCount = "info"

  //we create our streaming source from the topic info
  val streamSource: KStream[String, String]  = builder.stream(topicToCount)

  //we created a window store named "CountsWindowStore" that contains the counts for each key in 1-minute windows

  val countsError:KTable[Windowed[scala.Predef.String], java.lang.Long]  = streamSource.groupByKey()
                  .windowedBy(TimeWindows.of(60000))
                  .count(Materialized.as[scala.Predef.String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("CountsWindowStore") withKeySerde(Serdes.String()))




  //we write the count result to another output and provide overridden serialization methods for Long
  //.to("stream-grouped-output", Produced.with[scala.Predef.String, java.lang.Long])

  //val a=Produced.`with`(Windowed[Serdes.StringSerde,Serdes.LongSerde], Serdes.Long(),StreamPartitioner[Windowed[scala.Predef.String,java.lang.Long]])

  //countsError.toStream().to(Windowed[String],Serdes.Long() ,"stream-grouped-output")

  //Here we glance on our topology
  val  topology = builder.build()
  println(topology.describe())


  //we can now construct the Streams client
  val  streams: KafkaStreams = new KafkaStreams(topology, kafkfaStreamProperties)
  val latch = new CountDownLatch(3)

  // attach shutdown handler to catch control-c
  Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
    override  def run() {
      streams.close()   //we close the stream
      latch.countDown() //we wait 5s until all workers have completed before closing
    }
  })




  println("****************Step 2: starting the processor topology and Publish some data to the output topic ************")
  var timeFrom = 0L
  var timeTo = System.currentTimeMillis()
  streams.start()
  Thread.sleep(2000L)

  //we get the total of error each minutes
  while(true) {
      try {
            // Get the window store named "CountsWindowStore"
            val windowStore: ReadOnlyWindowStore[String, Long] = streams.store("CountsWindowStore", QueryableStoreTypes.windowStore[String, Long]())
            val keyToReadList = Range(25118129, 25118129 + (8*60)+1).map(x => x.toLong.toString) //

            for(keyToRead <- keyToReadList) {
                val iterator = windowStore.fetch(keyToRead, timeFrom, timeTo)
              //println(s"$keyToRead , in the for")
                while (iterator.hasNext) {
                //  println("in the while")
                  val next = iterator.next
                  println(s"Log type: $topicToCount, Timestamp:$keyToRead,  total events:${next.value}, windows timestamp:${next.key}")
                }
            }
            //we change our windows time
            timeFrom = timeTo
            timeTo = System.currentTimeMillis()

        } catch {
          case e: Throwable => e.printStackTrace()
        }
  }
}
