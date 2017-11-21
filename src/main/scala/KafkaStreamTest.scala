import Utils._
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.{Future, Promise}


case class NodataException(msg: String) extends Exception(msg)


object KafkaStreamTest extends App {

  val topicToCount = "error"
  val uriPath = s"/$topicToCount"
  //var futureAnswer = Promise[String]
  //futureAnswer.failure(NodataException("No data to print"))
  var answer = "NO DATA"

  println("**************** Step 0: Akka Http Configuration ************")

  def getAnswer(): String  = answer

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val serverSource = Http().bind(interface = "localhost", port = 8080)


  //we configure the handler
  val requestHandler: HttpRequest => Future[HttpResponse] = {
    case HttpRequest(GET,Uri.Path(uripath),_,_,_) => Future {
                        HttpResponse(entity= HttpEntity(ContentTypes.`text/html(UTF-8)`,getAnswer))
                      }

    case req: HttpRequest => Future {
      req.discardEntityBytes() // important to drain incoming HTTP Entity stream
      HttpResponse(404, entity = "Unknown resource!")
    }
  }



  //we treat the request
  val bindingFuture: Future[Http.ServerBinding] =
    serverSource.to(Sink.foreach { connection =>
      println("Accepted new connection from " + connection.remoteAddress)

      connection handleWithAsyncHandler requestHandler
      // this is equivalent to
      // connection handleWith { Flow[HttpRequest] map requestHandler }
    }).run()


  println("**************** Step 1: kafkastream Configuration ************")
  val streams = configureKafkaStream(topicToCount)



  println("**************** Step 2: starting the processor topology and Publish some data to the output topic ************")
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
                  val next = iterator.next
                  answer = s"Log type: $topicToCount, Timestamp:$keyToRead,  total events:${next.value}, windows timestamp:${next.key}"
                  println(answer)
                  Thread.sleep(3000L) //this will help for akka http. we need to have time for printing information
                }
            }
            //we change our windows time
            timeFrom = timeTo
            timeTo = System.currentTimeMillis()

        } catch {
          case e: Throwable => e.printStackTrace()
        }
  }

//we close the connection
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ => system.terminate()) // and shutdown when done

}
