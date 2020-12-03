package com.userbehavior

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map

//input
case class Order(OrderId: Long, Type: String, time:  Long)

//output
case class Msg(OrderId: Long, Type: String, code: Int)

object ordertimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.fromCollection(List(
      Order(1, "create", 1606073208),
      Order(2, "create", 1606073209),
      Order(2, "pay", 1606073250),
      Order(3, "create", 1606073211),
      Order(1, "pay", 1606074348),
      Order(4, "create", 1606074349)
    ))
      .assignAscendingTimestamps(_.time*1000)
      .keyBy(_.OrderId)


    val pattern = Pattern. begin[Order]("begin").where(_.Type == "create")
      .followedBy("next").where(_.Type == "pay").within(Time.minutes(15))

    val out = OutputTag[Msg]("OrderOutput")

    val patternStream = CEP.pattern(stream, pattern)
    val res = patternStream.select(out)(
      (pattern: Map[String, Iterable[Order]], timestamp: Long) =>{
        val timeoutId = pattern.getOrElse("begin", null).iterator.next().OrderId
        Msg(timeoutId, "Request-timed-out", 1)
      }
    )(
      (pattern: Map[String, Iterable[Order]]) =>{
        val successId = pattern.getOrElse("next", null).iterator.next().OrderId
        Msg(successId, "Success", 0)
      }
    )

    res.print()
    val timeoutStream = res.getSideOutput(out)
    timeoutStream.print()

    env.execute()
  }

}
