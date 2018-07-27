package com.cognitree.spark.samples.kafka_to_es

import com.cognitree.spark.data.SideInput
import com.cognitree.spark.es.{EsEnvConfig, EsStreamWriter}
import com.cognitree.spark.kafka.KafkaStreamReader
import com.cognitree.spark.streams.Pipeline
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods

object TestPipeline extends Pipeline[String, String] with KafkaStreamReader with EsStreamWriter {

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
      .map(str => {
        implicit val formats = org.json4s.DefaultFormats
        JsonMethods.parse(str).extract[Map[String, Any]]
      })
    val esEnvConfig = EsEnvConfig()
    write(writeStream, esEnvConfig, "test_index", "test_type")
    dStream
  }
}
