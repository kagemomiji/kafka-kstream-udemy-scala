package info.tri_comfort.kafka

import java.{util => ju}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology


class WordCount extends App{

    def createTopology: Topology = {
        val builder:StreamsBuilder = new StreamsBuilder

        val textLines: KStream[String,String] = builder.stream("word-count-input")

        val wordCounts: KTable[String,Long] = textLines
            .mapValues((k:String,v:String) => v.toLowerCase())
            .flatMapValues((k:String, v:String) => v.split("\\W+").toList)
            .selectKey((key,word) => word)
            .groupByKey
            .count()
           
        wordCounts.toStream.to("word-count-output")

        builder.build()
    }

    val config: ju.Properties =  {
        val p: ju.Properties = new ju.Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
        p
    }

    val topology:Topology = createTopology

    val streams: KafkaStreams = new KafkaStreams(topology,config)

    streams.start()
    System.out.println(streams.toString())

// shutdown hook to correctly close the streams application
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
}
