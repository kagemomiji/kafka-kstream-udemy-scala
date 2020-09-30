package info.tri_comfort.kafka

import java.{util => ju}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.KafkaStreams


object UserEventEnricherApp extends App{
    val config: ju.Properties = {
        val p = new ju.Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-enricher-app")
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        p
    }
    
    val builder: StreamsBuilder = new StreamsBuilder

    val userGlobalTable: GlobalKTable[String, String] = builder.globalTable("user-table")

    val userPurchases: KStream[String,String] = builder.stream("user-purchases")

    val userPurchasesEnrichedJoin: KStream[String, String] = userPurchases
        .join(userGlobalTable)(
            //Kstreamのなかのkey,valueのどのあたいをGlobalTableのkeyと結合するか
            (key, value) => key,
            (userPurchase, userInfo) => s"Purchase=${userPurchase} , UserInfo=[${userInfo}]"
        )
    userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join")

    val userPurchasesEnrichedLeftJoin: KStream[String,String] = userPurchases
        .leftJoin(userGlobalTable)(
            (key, value) => key,
            (userPurchase, userInfo) => {
                if ( userInfo != null){
                    s"Purchase=${userPurchase},UserInfo=[${userInfo}]"
                }else {
                    s"Purchase=${userPurchase},UserInfo=null"
                }
            }
        )
    userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join")    

    val streams :KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.cleanUp()
    streams.start()

    System.out.println(streams.toString())

// shutdown hook to correctly close the streams application
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })

  
}
