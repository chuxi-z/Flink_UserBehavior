package com.logindetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class Login(userId: Long, ip: String, Type: String, time: Long)

case class warningInfo(userId: Long, code: Int, Type: String, msg: String)

object loginDetect {
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
      .filter(_.Type == "error")
      .keyBy(_.userId) //classify by userid
      .process(new func())
      .print()

    env.execute()
  }

  class func extends KeyedProcessFunction[Long, Login, warningInfo]{
    lazy val state: ListState[Login] = getRuntimeContext.getListState(new ListStateDescriptor[Login]("state", classOf[Login]))

    override def processElement(i: Login, context: KeyedProcessFunction[Long, Login, warningInfo]#Context, collector: Collector[warningInfo]): Unit = {
      state.add(i)
      context.timerService().registerEventTimeTimer(i.time + 5 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Login, warningInfo]#OnTimerContext, out: Collector[warningInfo]): Unit = {
      val loginEvents: ListBuffer[Login] = ListBuffer()
      import scala.collection.JavaConversions._
      for (login <- state.get()){
        loginEvents += login
      }
      state.clear()

      if(loginEvents.length >= 5){
        val info = warningInfo(loginEvents.head.userId, 500, "error", "Exists danger behavior!!!")
        out.collect(info)
      }
    }
  }
}
