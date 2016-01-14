package io.confluent.examples.streams

import java.util.Properties

import io.confluent.examples.streams.utils.SystemTimestampExtractor
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KeyValue}
import org.apache.kafka.streams.{KafkaStreaming, StreamingConfig}

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

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    val uppercasedWithMap: KStream[Array[Byte], String] = textLines.map((key, value) => new KeyValue(key, value.toUpperCase()))

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    val originalAndUppercased: KStream[String, String] = textLines.map((key, value) => KeyValue.pair(value, value.toUpperCase()))

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
      settings.put(StreamingConfig.JOB_ID_CONFIG, "map-function-example")
      settings.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
      settings.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      settings.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      settings.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      settings.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[SystemTimestampExtractor])
      new StreamingConfig(settings)
    }
    val stream: KafkaStreaming = new KafkaStreaming(builder, streamingConfig)
    stream.start()
  }

}