package com.cognitree.spark.samples.kafka_to_kafka

import com.cognitree.spark.data.SideInput
import com.cognitree.spark.kafka.KafkaEnvConfig
import com.cognitree.spark.streams.PipelineExecutor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object Main {
  
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .getOrCreate()
    val context = new StreamingContext(spark.sparkContext, Seconds(5))
    logger.info("run: initialized spark streaming context")

    SideInput.cache()
    logger.info("run: initialized the side inputs")
    val kafkaConfig: KafkaEnvConfig = KafkaEnvConfig()
    // test pipeline
    val controlMessageKafkaStream: DStream[ConsumerRecord[String, String]] = TestPipeline
      .createInputStream(context, kafkaConfig, "test")
    val controlDataStream: DStream[String] = TestPipeline.valueStream(controlMessageKafkaStream)
    PipelineExecutor[String, String](context, kafkaConfig, TestPipeline, SideInput, controlDataStream)
    TestPipeline.commitOffsets(controlMessageKafkaStream)

    context.start()
    context.awaitTermination()
  }
}
