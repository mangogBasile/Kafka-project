import Utils._

object Consumer extends App {

  val TOPIC="logtest"

  //we read data in each partitition
  val list= List(1,2,3)
  list.par.map(index=>readKafkaPartition(TOPIC,index))


}