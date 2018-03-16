package a.kafka_streaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
  * Created by chaozhang on 2018/1/22.
  * 说明： 读取kafka的数据
  *
  * 自动维护kafka的offset，默认读取最新的offset，会丢失数据
  */
object KafkaConsumer {
  def main(args: Array[String]) {
    //当以jar包提交时，设置为local[*]会抛错（exitCode 13）。
    var masterUrl = "local[*]"
    if (args.length > 0){
      masterUrl = args(0)
    }

    //Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl)
      .setAppName("weike")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")


    //3.配置创建kafka输入流的所需要的参数，注意这里可以加入一些优化参数
    val brokers = "spark-0:6667,spark-1:6667,spark-2:6667"
    val groupid = "bigdata"
    val topics = Set("weike_3")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //4.创建kafka输入流0.10
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //把数据缓存为两份
    stream.map(record => (record.key().toString + record.value().toString)).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
