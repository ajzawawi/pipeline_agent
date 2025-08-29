package com.ajzawawi.pipeline.agent.config

final case class S3Config(
                         endpoint: String,
                         accessKey: String,
                         secretKey: String,
                         bucket: String,
                         objectKey: String
                         )
