/**
  * Copyright 2016 Confluent Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
  * in compliance with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License
  * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
  * or implied. See the License for the specific language governing permissions and limitations under
  * the License.
  */

package io.confluent.examples.streams

import java.lang.{Long => JLong}
import java.util.{Collections, Properties}

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to perform a join between a KStream and a
  * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
  *
  * See JoinLambdaIntegrationTest for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * JoinLambdaIntegrationTest.  One difference is that we switched from
  * BeforeClass/AfterClass (which must be `static`) to Before/After in this Scala example
  * to simplify the Scala/JUnit integration.
  */
class JoinScalaIntegrationTest extends AssertionsForJUnit {

  private var cluster: EmbeddedSingleNodeKafkaCluster = _
  private val userClicksTopic = "user-clicks"
  private val userRegionsTopic = "user-regions"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() = {
    cluster = new EmbeddedSingleNodeKafkaCluster()
    cluster.createTopic(userClicksTopic)
    cluster.createTopic(userRegionsTopic)
    cluster.createTopic(outputTopic)
  }

  @After
  def stopKafkaCluster() = {
    if (cluster != null) {
      cluster.stop()
    }
  }

  @Test
  def shouldCountClicksPerRegion() {

    // Input 1: Clicks per user (multiple records allowed per user).
    val userClicks: Seq[(String, Long)] = Seq(
      ("alice", 13L),
      ("bob", 4L),
      ("chao", 25L),
      ("bob", 19L),
      ("dave", 56L),
      ("eve", 78L),
      ("alice", 40L),
      ("fang", 99L)
    )

    // Input 2: Region per user (multiple records allowed per user).
    val userRegions: Seq[(String, String)] = Seq(
      ("alice", "asia"), /* Alice lived in Asia originally... */
      ("bob", "americas"),
      ("chao", "asia"),
      ("dave", "europe"),
      ("alice", "europe"), /* ...but moved to Europe some time later. */
      ("eve", "americas"),
      ("fang", "asia")
    )

    val expectedClicksPerRegion: Seq[(String, Long)] = Seq(
      ("europe", 13L),
      ("americas", 4L),
      ("asia", 25L),
      ("americas", 23L),
      ("europe", 69L),
      ("americas", 101L),
      ("europe", 109L),
      ("asia", 124L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zookeeperConnect())
      p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      // Explicitly place the state directory under /tmp so that we can remove it via
      // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
      // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
      // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
      // accordingly.
      p.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
      p
    }

    // Remove any state from previous test runs
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration)

    val builder: KStreamBuilder = new KStreamBuilder()

    // This KStream contains information such as "alice" -> 13L.
    //
    // Because this is a KStream ("record stream"), multiple records for the same user will be
    // considered as separate click-count events, each of which will be added to the total count.
    val userClicksStream: KStream[String, JLong] = builder.stream(stringSerde, longSerde, userClicksTopic)

    // This KTable contains information such as "alice" -> "europe".
    //
    // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
    // record key will be considered at the time when a new user-click record (see above) is
    // received for the `leftJoin` below.  Any previous region values are being considered out of
    // date.  This behavior is quite different to the KStream for user clicks above.
    //
    // For example, the user "alice" will be considered to live in "europe" (although originally she
    // lived in "asia") because, at the time her first user-click record is being received and
    // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
    // (which overrides her previous region value of "asia").
    val userRegionsTable: KTable[String, String] = builder.table(stringSerde, stringSerde, userRegionsTopic)

    // Compute the number of clicks per region, e.g. "europe" -> 13L.
    //
    // The resulting KTable is continuously being updated as new data records are arriving in the
    // input KStream `userClicksStream` and input KTable `userRegionsTable`.
    val clicksPerRegion: KTable[String, JLong] = userClicksStream
        // Join the stream against the table.
        //
        // Null values possible: In general, null values are possible for region (i.e. the value of
        // the KTable we are joining against) so we must guard against that (here: by setting the
        // fallback region "UNKNOWN").  In this specific example this is not really needed because
        // we know, based on the test setup, that all users have appropriate region entries at the
        // time we perform the join.
        .leftJoin(userRegionsTable, (clicks: JLong, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((user: String, regionWithClicks: (String, JLong)) => new KeyValue[String, JLong](regionWithClicks._1, regionWithClicks._2))
        // Compute the total per region by summing the individual click counts per region.
        .reduceByKey(
          (firstClicks: JLong, secondClicks: JLong) => firstClicks + secondClicks,
          stringSerde, longSerde, "ClicksPerRegionUnwindowedScala"
        )

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.to(stringSerde, longSerde, outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
    streams.start()

    // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
    // of the input data we produce below).
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(5000)

    //
    // Step 2: Publish user-region information.
    //
    // To keep this code example simple and easier to understand/reason about, we publish all
    // user-region records before any user-click records (cf. step 3).  In practice though,
    // data records would typically be arriving concurrently in both input streams/topics.
    val userRegionsProducerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p
    }

    val userRegionsProducer: Producer[String, String] = new KafkaProducer[String, String](userRegionsProducerConfig)
    userRegions.foreach(x =>
      userRegionsProducer.send(new ProducerRecord[String, String](userRegionsTopic, x._1, x._2)).get())
    userRegionsProducer.flush()
    userRegionsProducer.close()

    //
    // Step 3: Publish some user click events.
    //
    val userClicksProducerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
      p
    }

    val userClicksProducer: Producer[String, Long] = new KafkaProducer[String, Long](userClicksProducerConfig)
    userClicks.foreach(x =>
      userClicksProducer.send(new ProducerRecord[String, Long](userClicksTopic, x._1, x._2)).get())
    userClicksProducer.flush()
    userClicksProducer.close()

    // Give the stream processing application some time to do its work.
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(10000)
    streams.close()

    //
    // Step 4: Verify the application's output data.
    //
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "join-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
      p
    }
    val consumer: KafkaConsumer[String, Long] = new KafkaConsumer[String, Long](consumerConfig)
    consumer.subscribe(Collections.singletonList(outputTopic))

    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] = IntegrationTestUtils.readKeyValues(consumer)

    // We need to convert `expectedClicksPerRegion` so that we can compare it with `actualClicksPerRegion`.
    // (We wouldn't need to convert if we modified the Java-focused `IntegrationTestUtils` to be
    // more Scala friendly.)
    val exp = expectedClicksPerRegion.map { case (region, clicks) => new KeyValue(region, clicks) }
    import scala.collection.convert.wrapAsJava._
    assertThat(actualClicksPerRegion).containsExactlyElementsOf(exp)
  }

}