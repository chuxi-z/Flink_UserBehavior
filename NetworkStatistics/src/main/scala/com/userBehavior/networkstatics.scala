package com.userBehavior

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//input
case class ApacheLogEvent(ip: String, userid: String, eventTime: Long, method: String, url: String)

//output
case class UrlViews(url: String, windowEnd: Long, count: Long)

object networkstatics {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("/Users/chuxizhang/IdeaProjects/UserBehavior/NetworkStatistics/src/main/resources/apache.log")
      .map(line =>{
        val array = line.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = dateFormat.parse(array(3)).getTime
        ApacheLogEvent(array(0), array(1), timestamp, array(5), array(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new countAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
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

  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViews, String]{

    //define state variable
    lazy val urlStatus: ListState[UrlViews] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViews]("urlStatus", classOf[UrlViews]))

    override def processElement(i: UrlViews, context: KeyedProcessFunction[Long, UrlViews, String]#Context, collector: Collector[String]): Unit = {
      // save every data into state
      urlStatus.add(i)
      // register a timer, windowEnd + n start
      context.timerService().registerEventTimeTimer(i.windowEnd + 10*1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViews, String]#OnTimerContext, out: Collector[String]): Unit = {
      //get all views from state
      val allViews:ListBuffer[UrlViews] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlStatus.get()){
        allViews += urlView
      }

      urlStatus.clear()
      val sortViews = allViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      var res: StringBuilder = new StringBuilder
      res.append("=====================\n")
      res.append("Time: ").append(new Timestamp(timestamp - 10*1000)).append("\n")

      for (i <- sortViews.indices){
        val urlView: UrlViews = sortViews(i)

        res.append("No").append(i+1).append(":")
          .append(" URL: ").append(urlView.url)
          .append(" Views: ").append(urlView.count).append("\n")

      }

      res.append("=====================\n")
      Thread.sleep(500)
      out.collect(res.toString())
    }

  }

}
