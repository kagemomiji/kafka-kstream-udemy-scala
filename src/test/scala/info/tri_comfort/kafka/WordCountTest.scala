package info.tri_comfort.kafka

import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.streams.TopologyTestDriver
import java.{util => ju}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.streams.test.OutputVerifier
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

class WordCountTest extends AnyWordSpecLike 
    with Matchers
{
    "WordCountTestable" should {

        "dummy test" in {
            val dummy:String = "Du" + "mmy"
            dummy should be("Dummy")
        }

        "word counts are correct" in {
            val testDriver = createTestDriver
            try {
            val firstExample: String = "testing Kafka Stream"
            pushNewInputRecord(firstExample,testDriver)
            def readOutput() = {
                val output = testDriver.readOutput("word-count-output",new StringDeserializer, new LongDeserializer)
                output
            }
            val firstOutput = readOutput()
            firstOutput.key() should be("testing")
            firstOutput.value() should be(1L)
            val secondOutput = readOutput()
            secondOutput.key() should be("kafka")
            secondOutput.value() should be(1L)
            val thirdOutput = readOutput()
            thirdOutput.key() should be("stream")
            thirdOutput.value() should be(1L)

            pushNewInputRecord("kafka kafka KAFKA",testDriver)
            val firstOutputN = readOutput()
            firstOutputN.key() should be("kafka")
            firstOutputN.value() should be(2L)
            val secondOutputN = readOutput()
            secondOutputN.key() should be("kafka")
            secondOutputN.value() should be(3L)
            val thirdOutputN = readOutput()
            thirdOutputN.key() should be("kafka")
            thirdOutputN.value() should be(4L)
            } finally {
                testDriver.close()
            }

        }

    }

    def createTestDriver(): TopologyTestDriver = {
            val config: ju.Properties = new ju.Properties()
            config.put(StreamsConfig.APPLICATION_ID_CONFIG,"test")
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:1234")
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String.getClass().getName())

            val wordCountApp: WordCount = new WordCount()
            val topology: Topology = wordCountApp.createTopology

            val testDriver: TopologyTestDriver = new TopologyTestDriver(topology, config)
            testDriver
        }

    def recordFactory(): ConsumerRecordFactory[String,String] = {
        val stringSerializer = new StringSerializer
        new ConsumerRecordFactory[String,String](stringSerializer,stringSerializer)
    }

    def pushNewInputRecord(value: String, testDriver: TopologyTestDriver){
        testDriver.pipeInput(recordFactory.create("word-count-input", null, value))
    }



  
}
