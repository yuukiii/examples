package io.confluent.examples.streams

import java.util.Properties

import io.confluent.examples.streams.utils.SystemTimestampExtractor
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams._

/**
  * Demonstrates how to perform simple, state-less transformations via map functions.
  *
  * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
  * fields (such as personally identifiable information aka PII).
  *
  * Requires a version of Scala that supports Java 8 and SAM / Java lambda (e.g. Scala 2.12).
  */
class MapFunctionScalaExample {

  def main(args: Array[String]) {
    val builder: KStreamBuilder = new KStreamBuilder

    // Read the input Kafka topic into a KStream instance.
    val textLines: KStream[Array[Byte], String] = builder.stream(new ByteArrayDeserializer, new StringDeserializer, "TextLinesTopic")

    // Variant 1: using `mapValues`
    val uppercasedWithMapValues: KStream[Array[Byte], String] = textLines.mapValues(_.toUpperCase())

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // Alternatively you could also explicitly set the serializers instead of relying on the default
    // serializers specified in the streaming configuration (see further down below):
    //
    //     uppercasedWithMapValues.to("UppercasedTextLinesTopic"), new ByteArraySerializer, new StringSerializer)
    //
    uppercasedWithMapValues.to("UppercasedTextLinesTopic")

    // We are using implicit conversions to convert Scala's `Tuple2` into Kafka Streams' `KeyValue`.
    // This allows us to write streams transformations as, for example:
    //
    //    map((key, value) => (key, value.toUpperCase())
    //
    // instead of the more verbose
    //
    //    map((key, value) => new KeyValue(key, value.toUpperCase())
    //
    import KeyValueImplicits._

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    val uppercasedWithMap: KStream[Array[Byte], String] = textLines.map((key, value) => (key, value.toUpperCase()))

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    val originalAndUppercased: KStream[String, String] = textLines.map((key, value) => (value, value.toUpperCase()))

    // Write the results to a new Kafka topic "OriginalAndUppercased".
    //
    // In this case we must explicitly set the correct serializers because the default serializers
    // (cf. streaming configuration) do not match the type of this particular KStream instance.
    //
    // Note: Normally you would use a single instance of StringSerializer and pass it around.
    //       We are creating new instances purely for didactic reasons so that you do not need to
    //       jump around in the code too much in these examples.
    originalAndUppercased.to("OriginalAndUppercased", new StringSerializer, new StringSerializer)

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.JOB_ID_CONFIG, "map-function-example")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
      settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[SystemTimestampExtractor])
      settings
    }
    val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
    stream.start()
  }

}

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}