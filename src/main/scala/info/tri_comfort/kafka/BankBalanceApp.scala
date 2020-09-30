package info.tri_comfort.kafka

import java.{util => ju}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.streams.Topology
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.KafkaStreams
import com.fasterxml.jackson.databind.JsonNode
import io.circe.Json
import com.goyeau.kafka.streams.circe.CirceSerdes._
import io.circe.generic.auto._
import java.time.Instant

case class Transaction(name: String, amount: Int, time: String )
case class Balance(count: Int, balance: Int, time: String)


object BankBalanceApp extends App{

    val config: ju.Properties = {
        val p = new ju.Properties()
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app")
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0")
        p
    }

    val builder: StreamsBuilder = new StreamsBuilder

    val textLines: KStream[String, Transaction] = builder.stream[String,Transaction]("bank-transactions")

    val initialBalance = new Balance(0,0,Instant.ofEpochMilli(0).toString())

    val bankBalance: KTable[String, Balance] = textLines
        .groupByKey
        .aggregate(initialBalance)(
            (key, transaction, balance) => newBalance(transaction, balance)
        )

    bankBalance.toStream.to("bank-balance-exactly-once")

    val streams = new KafkaStreams(builder.build(),config)

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

    def newBalance(transaction: Transaction, balance: Balance): Balance = {

        val balanceEpoch: Long = Instant.parse(balance.time).toEpochMilli()
        val transactionEpoch : Long = Instant.parse(transaction.time).toEpochMilli()
        val newBalanceInstant: Instant = Instant.ofEpochMilli(math.max(balanceEpoch,transactionEpoch))

        val newBalance = new Balance(
            balance.count + 1,
            balance.balance + transaction.amount,
            newBalanceInstant.toString()
        )
        newBalance
    }

  
}
