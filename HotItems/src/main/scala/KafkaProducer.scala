import java.util.Properties
import scala.io
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile( "/Users/chuxizhang/IdeaProjects/UserBehavior/HotItems/src/main/file/UserBehavior.csv" )
    for( line <- bufferedSource.getLines() ){
      println(line)
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
