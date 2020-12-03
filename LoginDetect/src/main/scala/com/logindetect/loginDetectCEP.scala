package com.logindetect

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object loginDetectCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.fromCollection(List(
      Login(1, "127.0.0.1", "error", 1605970590),
      Login(1, "127.0.0.2", "error", 1605970591),
      Login(1, "127.0.0.3", "error", 1605970592),
      Login(1, "127.0.0.4", "error", 1605970593),
      Login(1, "127.0.0.4", "error", 1605970594),
      Login(2, "127.0.0.1", "success", 1605970595),
      Login(3, "127.0.0.1", "error", 1605970596)
    ))
      .assignAscendingTimestamps(_.time*1000)
      .keyBy(_.userId)

    //define a pattern
    val pattern = Pattern.begin[Login]("begin").where(_.Type == "error")
      .next("next").where(_.Type == "error")
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    //get error event from patternStream
    import scala.collection.Map
    val errorStream = patternStream.select(
      (p: Map[String, Iterable[Login]]) =>{
        val next = p.getOrElse("next", null).iterator.next()
        warningInfo(next.userId, 500, "error", "Exists danger behavior!!!")
      }
    )
      .print()

    env.execute()
  }
}
