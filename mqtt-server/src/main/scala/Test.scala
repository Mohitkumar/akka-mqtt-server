import scala.collection.mutable.Map

/**
  * Created by Mohit Kumar on 2/25/2017.
  */
object Test {
  def main(args: Array[String]) {
    val dataStore: Map[String, Any] = Map[String, Any]()
    dataStore += (("test","data"))
    println(dataStore.contains("test"))
  }
}
