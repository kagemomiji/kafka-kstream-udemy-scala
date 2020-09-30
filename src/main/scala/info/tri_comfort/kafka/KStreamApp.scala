package info.tri_comfort.kafka

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.{util => ju}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology
import org.apache.kafka.common.serialization.Serde



object KStreamApp extends App {
   
    //create ktable

    val props: ju.Properties= {
        val p = new ju.Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "color-favorite-application")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0")
        p
    }


    val builder: StreamsBuilder = new StreamsBuilder

    val textLines: KStream[String, String] = builder.stream[String,String]("favourite-colour-input")

    val userAndColours: KStream[String,String] = textLines
    .filter((key: String, value: String) => value.contains(","))
    .selectKey[String]((key:String, value:String) => value.split(",")(0).toLowerCase)
    .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
    .filter((user:String, colour:String) => List("green","blue","red").contains(colour))
    .peek((key, value) => println(s"key: $key value $value"))

    val intermediateTopic: String = "user-keys-and-colours"

    userAndColours.to(intermediateTopic)

    val usersAndColoursTable: KTable[String, String] = builder.table(intermediateTopic)


    val favoriteColours: KTable[String, Long] = usersAndColoursTable
        .groupBy((_, colour) => (colour, colour))
        .count()
    
    val favoriteColoursAgg: KTable[String, Long] = usersAndColoursTable
        .groupBy((_, value) => (value, value))
        .aggregate(0L)(
            (key,value,aggValue) => aggValue + value.length,
            (key,oldValue, aggValue) => aggValue - oldValue.length
        )

    val favoriteColoursRed: KTable[String, Long] = usersAndColoursTable
        .groupBy(( _, value) => (value, value.length.toLong))
        .reduce((_ + _), (_ - _))
        
        
        //.groupBy((user, colour) => (colour, colour))
        //.count()
    
    favoriteColours.toStream.to("favourite-colour-output")

    val streams = new KafkaStreams(builder.build(),props)

    streams.cleanUp()
    streams.start()

    // print the topology
    streams.localThreadsMetadata().forEach(t => System.out.print(t.toString))

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
 
}
