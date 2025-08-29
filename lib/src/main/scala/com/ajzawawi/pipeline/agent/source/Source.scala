package com.ajzawawi.pipeline.agent.source

import com.ajzawawi.pipeline.agent.csv.CsvWriter
import com.ajzawawi.pipeline.agent.validator.file.{FileValidationOutcome, FileValidator}
import com.ajzawawi.pipeline.agent.validator.row._

import java.nio.charset.StandardCharsets
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.commons.io.IOUtils
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import java.nio.file.{Files, Path, StandardCopyOption, AtomicMoveNotSupportedException}

import java.util.concurrent.{ThreadFactory, ThreadPoolExecutor, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.ConcurrentHashMap

final class Source(val cfg: SourceConfig) extends AutoCloseable {
  @volatile private var running = false
  private val watcher = FileSystems.getDefault.newWatchService()
  private var watchKey: WatchKey = _

  // Reasonable defaults for I/O-bound work (gzip + file I/O)
  private val workerThreads = cfg.concurrency.map(_.threads).getOrElse(2)
  private val maxQueueSize = cfg.concurrency.map(_.queueSize).getOrElse(1024)

  private val queue  = new LinkedBlockingQueue[Runnable](maxQueueSize)

  private val workerTf: ThreadFactory = (r: Runnable) => {
    val t = new Thread(r, s"source-${cfg.name}-worker")
    t.setDaemon(true);
    t
  }

  private val workers = new ThreadPoolExecutor(
    workerThreads, workerThreads,
    0L, TimeUnit.MILLISECONDS,
    queue,
    workerTf,
    // Note, let's keep things moving without dropping tasks, so bursts will slow down the watch loop a bit
    // We can re-visit the back pressure policy after testing the bigger file sizes
    new ThreadPoolExecutor.CallerRunsPolicy()
  )

  private val inFlight = ConcurrentHashMap.newKeySet[Path]()

  // UTC timestamp for archive filenames
  private val tsFmt =
    DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
      .withZone(ZoneOffset.UTC)

  def createDirectories(cfg: SourceConfig): Path = {
    Files.createDirectories(cfg.inbox)
    Files.createDirectories(cfg.output.clean)
    Files.createDirectories(cfg.output.reject)
    Files.createDirectories(cfg.output.rowErrors)
    Files.createDirectories(cfg.archive.accepted)
    Files.createDirectories(cfg.archive.rejected)
    Files.createDirectories(cfg.archive.failed)
  }

  def start(): Unit = {
    // 1) register watch first to avoid missing new files
    createDirectories(cfg)

    watchKey = cfg.inbox.register(watcher, ENTRY_CREATE)

    // 2) process startup backlog (existing files)
    scanBacklog()

    // 3) start the event loop
    running = true
    val t = new Thread(() => loop(), s"source-${cfg.name}-watcher")
    t.setDaemon(true)
    t.start()
    println(s"[source:${cfg.name}] watching ${cfg.inbox}")
  }

  def stop(): Unit = { running = false; watcher.close() }

  override def close(): Unit = stop()

  // Process all pre-existing candidate files once at startup (oldest first)
  private def scanBacklog(): Unit = {
    val it = Files.list(cfg.inbox)
    try {
      it.iterator().asScala
        .filter(Files.isRegularFile(_))
        .filter(isCandidate)
        .toSeq
        .sortBy(p => Files.getLastModifiedTime(p).toMillis)
        .foreach(submitIfNew)
    } finally it.close()
  }

  // Queue if not already queued/processing; clear after run
  private def submitIfNew(file: Path): Unit = {
    if (inFlight.add(file)) {
      workers.submit(new Runnable {
        override def run(): Unit = {
          try safeProcess(file)
          finally inFlight.remove(file)
        }
      })
    }
  }


  // This will ignore any files that don't match the extensions we specified
  private def isCandidate(p: Path): Boolean = {
    val name = p.getFileName.toString
    cfg.allowedExtensions.exists(name.endsWith)
  }

  private def loop(): Unit = {
    try {
      while (running) {
        val key = watcher.take()
        key.pollEvents().asScala.foreach { ev =>
          if (ev.kind() == ENTRY_CREATE) {
            val filename = ev.context().asInstanceOf[Path]
            val file = cfg.inbox.resolve(filename)
            if (isCandidate(file)) safeProcess(file)
          }
        }
        key.reset()
      }
    } catch {
      case _: ClosedWatchServiceException => () // normal on stop()
    }
  }

  private def safeProcess(file: Path): Unit = {
    try {
      waitUntilStable(file)
      processFile(file)
    } catch {
      case t: Throwable =>
        println(s"[source:${cfg.name}] ERROR processing ${file.getFileName}: ${t.getMessage}")
        if (Files.exists(file)) {
          val archived = archiveCompressed(cfg.archive.failed, file)
          println(s"[source:${cfg.name}] archived (failed, gz) → $archived")
        }
    }
  }

  private def waitUntilStable(p: Path, polls: Int = 3, sleepMs: Long = 250L): Unit = {
    var last = -1L
    var stable = 0
    while (stable < polls) {
      val now = Files.size(p)
      if (now == last) stable += 1 else { stable = 0; last = now }
      Thread.sleep(sleepMs)
    }
  }

  private def processFile(file: Path): Unit = {
    val fileRules = cfg.validator.file

    FileValidator.validateFile(file.toString, fileRules) match {
      case FileValidationOutcome.Rejected(reasons) =>
        writeFileRejectReport(file, reasons)
        if (Files.exists(file)) {
          val archived = archiveCompressed(cfg.archive.rejected, file)
          println(s"[source:${cfg.name}] archived (rejected, gz) → $archived")
        }

      case FileValidationOutcome.Accepted(_) =>
        val lines     = Using.resource(scala.io.Source.fromFile(file.toFile, "UTF-8"))(_.getLines().toList)
        val dataLines = DataRows.extract(lines)

        val rowRules  = cfg.validator.row
        val outcomes  = dataLines.zipWithIndex.map { case (line, i) =>
          RowValidator.validateRow(line, i + 1, rowRules)
        }

        val (valid, invalid) =
          outcomes.foldLeft((Vector.empty[RowValidationOutcome.Valid], Vector.empty[RowValidationOutcome.Invalid])) {
            case ((vAcc, iAcc), v: RowValidationOutcome.Valid)   => (vAcc :+ v, iAcc)
            case ((vAcc, iAcc), i: RowValidationOutcome.Invalid) => (vAcc, iAcc :+ i)
          }

        writeCleanCsv(file, valid, rowRules.delimiter)
        writeRowErrors(file, invalid)

        if (Files.exists(file)) {
          val archived = archiveCompressed(cfg.archive.accepted, file)
          println(s"[source:${cfg.name}] archived (accepted, gz) → $archived")
        }
    }
  }

  private def writeFileRejectReport(file: Path, reasons: List[String]): Unit = {
    val rep = cfg.output.reject.resolve(file.getFileName.toString + ".rejected.txt")
    val body =
      s"""|Rejected at ${Instant.now()}
          |Source: ${cfg.name}
          |File:   ${file.getFileName}
          |Reasons:
          |${reasons.map(r => s"- $r").mkString("\n")}
          |""".stripMargin
    Files.write(rep, body.getBytes(StandardCharsets.UTF_8))
    println(s"[source:${cfg.name}] rejected ${file.getFileName}: ${reasons.mkString("; ")}")
  }

  private def writeCleanCsv(file: Path, valids: Seq[RowValidationOutcome.Valid], delimiter: Char): Unit = {
    val out = cfg.output.clean.resolve(file.getFileName.toString.replaceAll("\\.[^.]+$", "") + ".clean.csv")
    val rows = valids.iterator.map(_.cells) // Vector[String]
    CsvWriter.write(out, rows, header = cfg.output.header, delimiter = delimiter)
    println(s"[source:${cfg.name}] clean → ${out.getFileName} (${valids.size} rows)")
  }

  private def writeRowErrors(file: Path, invalids: Seq[RowValidationOutcome.Invalid]): Unit = {
    if (invalids.isEmpty) return
    val errPath = cfg.output.rowErrors.resolve(file.getFileName.toString + ".row_errors.tsv")
    val header  = "rowNum\tcolIdx\tcode\tmessage\trowPreview"
    val lines   = invalids.flatMap { inv =>
      val preview = inv.cells.mkString("|")
      inv.errors.map { e =>
        val col = e.colIdx.map(_.toString).getOrElse("")
        s"${inv.rowNum}\t$col\t${e.code}\t${e.message}\t$preview"
      }
    }
    val all = (header +: lines).mkString("\n").getBytes(StandardCharsets.UTF_8)
    Files.write(errPath, all)
    println(s"[source:${cfg.name}] row errors → ${errPath.getFileName} (${invalids.size} bad rows)")
  }

  /** Ensure a unique target path in dir (append .1, .2, ...) */
  private def uniquePath(dir: Path, baseName: String): Path = {
    var i = 0
    var p = dir.resolve(baseName)
    while (Files.exists(p)) {
      i += 1; p = dir.resolve(s"$baseName.$i")
    }
    p
  }

  private def archiveCompressed(dir: Path, file: Path): Path = {
    Files.createDirectories(dir)
    val base = file.getFileName.toString
    val ts = tsFmt.format(java.time.Instant.now())
    val target = uniquePath(dir, s"$base.$ts.gz")
    val tmp = target.resolveSibling(target.getFileName.toString + ".part")

    try {
      Using.Manager { use =>
        val in = use(Files.newInputStream(file))
        val out = use(new GzipCompressorOutputStream(Files.newOutputStream(tmp)))
        IOUtils.copy(in, out)
      }

      // publish atomically when supported
      try Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
      catch {
        case _: AtomicMoveNotSupportedException =>
          Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
      }

      // We'll get rid of the original after success
      Files.deleteIfExists(file)
      target
    } catch {
      case e: Throwable =>
        // Our best effort clean up, because boy scout rule
      Files.deleteIfExists(tmp)
        throw e
    }
  }

}
