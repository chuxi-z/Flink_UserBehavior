
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//input data
case class UserBehavior(userId:Long, itemId:Long, categoryId: Int, behavior:String, timeStamp:Long)

//output data
case class ItemViewCount(itemId:Long, windowEnd:Long, count: Long )


object HotItems {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val stream = environment.readTextFile("/Users/chuxizhang/IdeaProjects/UserBehavior/HotItems/src/main/file/UserBehavior.csv")
      .map(line => {
        val strings = line.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHot(3))
      .print()

    environment.execute("hot item")
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc1 + acc
  }

  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      var itemId = key.asInstanceOf[Tuple1[Long]].f0
      var count = input.iterator.next()

      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  class TopNHot(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      context.timerService.registerEventTimeTimer(i.windowEnd+1)

    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //get all goods Info
      var allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems += item
      }

      //clear all data, release space
      itemState.clear()

      //sort and take top N
      val sortItem = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      //format data
      val res = new StringBuilder
      res.append("=====================================\n")
      res.append("Time: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortItem.indices){
        val it = sortItem(i)

        res.append("No").append(i).append(":")
          .append(" GoodsId=").append(it.itemId)
          .append(" ViewCount=").append(it.count).append("\n")
      }
      res.append("=====================================\n\n")

      Thread.sleep(1000)
      out.collect(res.toString())
    }
  }


}
