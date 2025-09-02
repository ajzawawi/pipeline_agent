package com.ajzawawi.pipeline.agent

import com.ajzawawi.pipeline.agent.source.{Source, SourceConfigLoader}
import com.typesafe.scalalogging.LazyLogging

object PipelineAgent extends LazyLogging {

  def run(): Unit = {
    val sources = SourceConfigLoader.loadAll().map(cfg => new Source(cfg))

    // start all watchers
    sources.foreach(_.start())

    // graceful shutdown
    sys.addShutdownHook {
      sources.foreach(_.stop())
      println("[agent] shutdown complete")
    }

    logger.info(s"[agent] started ${sources.size} source(s). Press Ctrl+C to exit.")
    // keep main alive
    while (true) Thread.sleep(60_000)
  }
}

//object PipelineAgent extends App {
//  val config: AgentConfig = ConfigFactory.load().as[AgentConfig]("agent")
//  val s3 = config.s3
//
//  val client = MinioClient.builder()
//    .endpoint(s3.endpoint)
//    .credentials(s3.accessKey, s3.secretKey)
//    .build()
//
//  // Ensure bucket exists
//  val exists = client.bucketExists(BucketExistsArgs.builder().bucket(s3.bucket).build())
//  if (!exists) {
//    client.makeBucket(MakeBucketArgs.builder().bucket(s3.bucket).build())
//    println(s"Bucket '${s3.bucket}' created")
//  } else {
//    println(s"Bucket '${s3.bucket}' already exists")
//  }
//
//  val tmp = Files.createTempFile("hello-minio-", ".txt")
//  Files.writeString(tmp, "Hello from PipelineAgent!")
//
//  client.uploadObject(
//    UploadObjectArgs.builder()
//      .bucket(s3.bucket)
//      .`object`(s3.objectKey)
//      .filename(tmp.toString)
//      .build()
//  )
//
//  Files.deleteIfExists(tmp)
//
//  println(s"Uploaded s3://${s3.bucket}/${s3.objectKey}")
//
//  client.close()
//
//}
