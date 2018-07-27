package com.cognitree.spark.samples.kafka_to_mongo

import com.cognitree.spark.data.SideInput
import com.cognitree.spark.kafka.KafkaStreamReader
import com.cognitree.spark.mongo.{MongoDocument, MongoEnvConfig, MongoStreamWriter}
import com.cognitree.spark.streams.Pipeline
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.bson.Document

object TestPipeline extends Pipeline[String, String] with KafkaStreamReader with MongoStreamWriter {

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
      .map(str => new MongoDocument("out", "col", Document.parse(str)))
    val mongoEnvConfig = MongoEnvConfig()
    write(writeStream, mongoEnvConfig)
    dStream
  }
}
