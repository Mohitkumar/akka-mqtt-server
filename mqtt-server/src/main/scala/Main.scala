import io.{Channel, Server}

/**
  * Created by Mohit Kumar on 2/19/2017.
  */
object Main {
  def main(args: Array[String]) {
    Server.start()
    /*val ch = Channel(5)
    ch.put("test","value")
    println(ch.get("test"))
    ch.put("data",1)
    println(ch.getAsInt("data"))*/
  }
}
