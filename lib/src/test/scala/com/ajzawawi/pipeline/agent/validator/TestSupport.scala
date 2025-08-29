package com.ajzawawi.pipeline.agent.validator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

object TestSupport {
  def withTempFile(prefix: String, contents: String)(f: Path => Any): Unit = {
    val p = Files.createTempFile(prefix, ".txt")
    try {
      Files.write(p, contents.getBytes(StandardCharsets.UTF_8))
      f(p)
    } finally {
      Files.deleteIfExists(p)
    }
  }
}
