package com.ajzawawi.pipeline.agent.testsupport

import com.ajzawawi.pipeline.agent.source._
import com.ajzawawi.pipeline.agent.validator.file._
import com.ajzawawi.pipeline.agent.validator.row._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait SourceTestFixtures {
  def withTempDir(prefix: String)(f: Path => Unit): Unit = {
    val dir = Files.createTempDirectory(prefix)
    try f(dir)
    finally {
      // recursive delete (best-effort)
      def rm(p: Path): Unit = {
        if (Files.exists(p)) {
          if (Files.isDirectory(p)) {
            val s = Files.list(p)
            try s.iterator().asScala.foreach(rm) finally s.close()
          }
          Files.deleteIfExists(p)
        }
      }
      rm(dir)
    }
  }

  def write(path: Path, content: String): Path = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    path
  }

  def list(dir: Path): Seq[Path] = {
    if (!Files.exists(dir)) Seq.empty
    else {
      val s = Files.list(dir)
      try s.iterator().asScala.toSeq finally s.close()
    }
  }

  def awaitTrue(desc: String)(
    cond: => Boolean,
    timeout: FiniteDuration = 5.seconds,
    poll: FiniteDuration = 50.millis
  ): Unit = {
    val deadline = timeout.fromNow
    var ok = cond
    while (!ok && deadline.hasTimeLeft()) {
      Thread.sleep(poll.toMillis)
      ok = cond
    }
    assert(ok, s"Timed out waiting for: $desc")
  }

  def sampleFileRules: FileRules =
    FileRules(
      requireHeaderRow = true,
      headerPrefix     = "UHDR",
      trailerPrefix    = "Total",
      expectedColumns  = Some(Seq("ENTY", "CURR", "QTY", "DATE")),
      columnDelimiter  = ','
    )

  def sampleRowRules: RowRules =
    RowRules(
      delimiter    = ',',
      expectedCols = Some(4),
      columns = List(
        StringRule(index = 0, notBlank = true, minLen = Some(2), maxLen = Some(20)),
        StringRule(index = 1, notBlank = true, minLen = Some(3), maxLen = Some(3), regex = Some("^[A-Z]{3}$")),
        DecimalRule(index = 2, notBlank = true, sign = SignRule.Positive, min = Some(BigDecimal(0))),
        DateRule   (index = 3, notBlank = true, dateFormat = "yyyy-MM-dd")
      )
    )

  def makeConfig(root: Path): SourceConfig = {
    val inbox    = root.resolve("inbox")
    val outClean = root.resolve("out/clean")
    val outReject= root.resolve("out/reject")
    val outErr   = root.resolve("out/errors")
    val arcOk    = root.resolve("archive/accepted")
    val arcRej   = root.resolve("archive/rejected")
    val arcFail  = root.resolve("archive/failed")

    val output   = OutputPaths(outClean, outReject, outErr, header = Some(Seq("ENTY","CURR","QTY","DATE")))
    val archive  = ArchivePaths(arcOk, arcRej, arcFail)
    val validator= ValidatorCfg(sampleFileRules, sampleRowRules)

    SourceConfig(
      name = "TestSource",
      inbox = inbox,
      output = output,
      validator = validator,
      allowedExtensions = Set(".txt"),
      archive = archive
    )
  }

  def withStartedSource(cfg: SourceConfig)(test: Source => Unit): Unit = {
    val src = new Source(cfg)
    try { src.start(); test(src) } finally src.stop()
  }
}
