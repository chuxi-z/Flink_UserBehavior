package com.userBehavior

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//input
case class ApacheLogEvent(ip: String, userid: String, eventTime: Long, method: String, url: String)

//output
case class UrlViews(url: String, windowEnd: Long, count: Long)

object networkstatics {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.readTextFile("/Users/chuxizhang/IdeaProjects/UserBehavior/NetworkStatistics/src/main/resources/apache.log")
      .map(line =>{
        val array = line.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = dateFormat.parse(array(3)).getTime
        ApacheLogEvent(array(0), array(1), timestamp, array(6), array(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .filter(_.method == "GET")
      .keyBy("url")
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new countAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Network statics")
  }

  class countAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc1 + acc
  }

  class WindowResultFunction extends WindowFunction[Long, UrlViews, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViews]): Unit = {
      val url: String = key
      val count = input.iterator.next()
      out.collect(UrlViews(url, window.getEnd, count))
    }
  }

}
