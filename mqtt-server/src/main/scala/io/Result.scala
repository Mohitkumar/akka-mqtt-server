package io

/**
  * Created by Mohit Kumar on 2/19/2017.
  */
trait Result[+T] {
  def value:T
  var numberOfByteConsumed: Int
}
case class ResultObj[A](valu:A,numberOfBytesRead:Int) extends Result[A]{
  override def value = valu
  override var numberOfByteConsumed = this.numberOfBytesRead
}
