package com.ajzawawi.pipeline.agent.source

import com.ajzawawi.pipeline.agent.testsupport.SourceTestFixtures
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.{Files, Path, Paths}

class SourceSpec extends AnyFunSpec with SourceTestFixtures {

  describe("Source (startup backlog + archiving)") {

    it("processes a valid backlog file: writes clean CSV and archives original as .gz under accepted") {
      withTempDir("source_valid_") { root =>
        val cfg = makeConfig(root)

        // Prepare a valid file BEFORE start() so it is picked up by scanBacklog()
        val inFile = cfg.inbox.resolve("input1.txt")
        val txt =
          """UHDR 2025240
            |ENTY, CURR, QTY, DATE
            |AAPL, USD, 100, 2025-08-25
            |Total 1
            |""".stripMargin
        Files.createDirectories(cfg.inbox)
        write(inFile, txt)

        val src = new Source(cfg)
        try {
          src.start()

          // Expect clean CSV
          val cleanName = "input1.clean.csv"
          val cleanPath = cfg.output.clean.resolve(cleanName)
          awaitTrue(s"clean CSV $cleanName exists")(Files.exists(cleanPath))

          // Expect original archived (gz) to accepted
          awaitTrue("accepted archive .gz exists") {
            list(cfg.archive.accepted).exists(p =>
              p.getFileName.toString.startsWith("input1.txt.") && p.getFileName.toString.endsWith(".gz"))
          }

          // Original should be gone
          assert(!Files.exists(inFile))

          // No row error file should exist for this valid input
          val errFiles = list(cfg.output.rowErrors).filter(_.getFileName.toString.endsWith(".row_errors.tsv"))
          assert(errFiles.isEmpty)
        } finally {
          src.stop()
        }
      }
    }

    it("rejects an invalid backlog file: writes reject report and archives original as .gz under rejected") {
      withTempDir("source_reject_") { root =>
        val cfg = makeConfig(root)

        // Invalid: missing header control on first non-empty line
        val inFile = cfg.inbox.resolve("bad1.txt")
        val txt =
          """ENTY, CURR, QTY, DATE
            |AAPL, USD, 100, 2025-08-25
            |Total 1
            |""".stripMargin
        Files.createDirectories(cfg.inbox)
        write(inFile, txt)

        val src = new Source(cfg)
        try {
          src.start()

          // Reject report should be produced
          val rejectReport = cfg.output.reject.resolve("bad1.txt.rejected.txt")
          awaitTrue("reject report exists")(Files.exists(rejectReport))

          // No clean CSV should exist
          val cleanPath = cfg.output.clean.resolve("bad1.clean.csv")
          assert(!Files.exists(cleanPath))

          // Archived gz under rejected
          awaitTrue("rejected archive .gz exists") {
            list(cfg.archive.rejected).exists(p =>
              p.getFileName.toString.startsWith("bad1.txt.") && p.getFileName.toString.endsWith(".gz"))
          }

          // Original should be gone
          assert(!Files.exists(inFile))
        } finally {
          src.stop()
        }
      }
    }

    it("ignores non-candidate extensions in backlog") {
      withTempDir("source_ignore_") { root =>
        val cfg = makeConfig(root)

        // Put a .log file in inbox—should be ignored
        val ignored = cfg.inbox.resolve("note.log")
        Files.createDirectories(cfg.inbox)
        write(ignored, "some content\n")

        val src = new Source(cfg)
        try {
          src.start()

          // Give a short grace period then assert nothing was produced
          Thread.sleep(250)
          assert(Files.exists(ignored), "ignored file should remain unprocessed")
          assert(list(cfg.output.clean).isEmpty)
          assert(list(cfg.archive.accepted).isEmpty)
          assert(list(cfg.archive.rejected).isEmpty)
        } finally {
          src.stop()
        }
      }
    }
  }
}
