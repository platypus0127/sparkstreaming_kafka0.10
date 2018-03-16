package a.kafka_streaming

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Created by chaozhang on 2018/1/26.
  * 向kafka里生产数据
  *
  * 准备步骤：创建topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper spark-1:2181 --replication-factor 2 --partitions 2 --topic weike
  *
  */
object KafkaProducer {


  def main(args: Array[String]): Unit ={
    val topic = "weike_4"
//开源默认的端口是9092，ambari版本默认的是：6667
    val brokers = "spark-0:6667,spark-1:6667"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)

    val producer = new Producer[String, String](kafkaConfig)
    println("Message sent:" + "开始发送消息")

    var i = 0
    while(true){
      i = i + 1
      //produce event message
      producer.send(new KeyedMessage[String, String](topic, "weixin",  i.toString))
      println("Message sent:" + i)


      Thread.sleep(5000)
    }



  }

}
