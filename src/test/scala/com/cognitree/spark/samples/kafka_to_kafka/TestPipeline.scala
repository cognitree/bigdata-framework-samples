package com.cognitree.spark.samples.kafka_to_kafka

import com.cognitree.spark.data.SideInput
import com.cognitree.spark.kafka.{KafkaEnvConfig, KafkaStreamReader, KafkaStreamWriter}
import com.cognitree.spark.streams.Pipeline
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object TestPipeline extends Pipeline[String, String] with KafkaStreamReader with KafkaStreamWriter {

  /**
    * Parse the stream read from input source
    *
    * @param recordAsString the record as a string
    * @return
    */
  override def parse(recordAsString: String): Array[String] = Array(recordAsString)

  override def process(context: StreamingContext, streamCtx: Any,
                       sideInput: SideInput, dStream: DStream[String]): DStream[String] = {
    val writeStream = dStream
      .map(str => new ProducerRecord("out", "", str))
    write(writeStream, streamCtx.asInstanceOf[KafkaEnvConfig])
    dStream
  }
}
