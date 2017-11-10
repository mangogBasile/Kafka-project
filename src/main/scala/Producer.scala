import org.apache.kafka.clients.producer._
import scala.util.{Try, Success, Failure}
import Utils._


object Producer extends App {

  val TOPIC="logtest"

  //we create the topic
  createTopic(topicName = TOPIC, numPartitions = 4)
  Thread.sleep(10)

  //We create the producer
  val producer = new KafkaProducer[String, String](getkafkaProducerProperties)



  //we read the log file and we write to events into kafka topic with a partitionning by  key
  val logFile = readTextFileWithTry("/Users/basile/Documents/logiciels/data/host.log")

  logFile match {
      //Here we try to retrieve all lines
    case Success(lines) => lines.foreach { str =>
                                                  val pattern = "(\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}) .*".r
                                                  //we write each event into different partition in kafka
                                                  str match {
                                                    case pattern(timestamp) => sendDataToKafka(producer,TOPIC, timestamp, str)
                                                    case _ => println(s"Timestamp doesn't match, $str")
                                                  }
    }
    case Failure(s) => println(s"Failed, message is: $s")
  }


  //we stop the producer process
  producer.close()
}
