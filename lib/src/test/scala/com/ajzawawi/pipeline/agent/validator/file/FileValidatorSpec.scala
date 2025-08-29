package com.ajzawawi.pipeline.agent.validator.file

import com.ajzawawi.pipeline.agent.validator.TestSupport.withTempFile
import org.scalatest.funspec.AnyFunSpec

class FileValidatorSpec extends AnyFunSpec {

  private val rules = FileRules(
    requireHeaderRow = true,
    headerPrefix = "UHDR",
    trailerPrefix = "Total",
    expectedColumns = Some(Seq("ENTY", "CURR", "QTY", "DATE")),
    columnDelimiter = ',')

  describe("FileValidator.validateFile") {

    it("accepts when the column header names exactly match expectedColumns") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |AAPL, USD, 100, 2025-08-25
          |Total 1
          |""".stripMargin

      withTempFile("header_match_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        out match {
          case FileValidationOutcome.Accepted(n) =>
            assertResult(1)(n) // one data row
          case other =>
            fail(s"Expected Accepted(1), got $other")
        }
      }
    }

    it("rejects when the column header names do not match expectedColumns") {
      val txt =
        """UHDR 2025240
          |AAPL, USD, 100, 2025-08-25
          |Total 1
          |""".stripMargin

      withTempFile("header_mismatch_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.startsWith("Column header missing or mismatched")))
      }
    }

    it("accepts a valid file with two data rows and trailer Total 2") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |AAPL, USD, 100, 2025-08-25
          |MSFT, USD, 50, 2025-08-26
          |Total 2
          |""".stripMargin

      withTempFile("valid_two_rows_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Accepted])
        val n = out.asInstanceOf[FileValidationOutcome.Accepted].dataRowCount
        // 5 non-empty lines → 5 - 3 = 2 data rows
        assertResult(2)(n)
      }
    }

    it("rejects when trailer count mismatches (Total says 3 but actual is 2)") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |a,
          |MSFT, USD, 50, 2025-08-26
          |Total 3
          |""".stripMargin

      withTempFile("mismatch_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.contains("Trailer count 3 does not match actual 2")))
      }
    }

    it("rejects bad trailer format") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |AAPL, USD, 100, 2025-08-25
          |Total: 1
          |""".stripMargin

      withTempFile("bad_trailer_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.startsWith("Bad trailer format.")))
      }
    }

    it("rejects when trailer line is missing") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |AAPL, USD, 100, 2025-08-25
          |""".stripMargin

      withTempFile("missing_trailer_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.contains("Missing trailer line"))
      }
    }

    it("rejects when header control UHDR is missing") {
      val txt =
        """ENTY, CURR, QTY, DATE
          |AAPL, USD, 100, 2025-08-25
          |Total 1
          |""".stripMargin

      withTempFile("missing_u_hdr_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.contains("Missing header control 'UHDR YYYYDDD'")))
      }
    }

    it("rejects when the column headings row is missing (second line is blank)") {
      val txt =
        """UHDR 2025240
          |AAPL, USD, 100, 2025-08-25
          |Total 1
          |""".stripMargin

      withTempFile("missing_header_row_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.contains("Column header missing or mismatched")))
      }
    }

    it("accepts a file with zero data rows (Total 0) when only UHDR, header, trailer exist") {
      val txt =
        """UHDR 2025240
          |ENTY, CURR, QTY, DATE
          |Total 0
          |""".stripMargin

      withTempFile("zero_rows_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Accepted])
        val n = out.asInstanceOf[FileValidationOutcome.Accepted].dataRowCount
        assertResult(0)(n) // 3 non-empty lines → 3 - 3 = 0
      }
    }

    it("ignores blank lines when counting data rows") {
      val txt =
        """UHDR 2025240
          |
          |ENTY, CURR, QTY, DATE
          |
          |AAPL, USD, 100, 2025-08-25
          |
          |MSFT, USD, 50, 2025-08-26
          |
          |Total 2
          |""".stripMargin

      withTempFile("blanks_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Accepted])
        val n = out.asInstanceOf[FileValidationOutcome.Accepted].dataRowCount
        assertResult(2)(n)
      }
    }

    it("aggregates multiple failures (no UHDR + bad trailer)") {
      val txt =
        """ENTY, CURR, QTY, DATE
          |Row A
          |Total: two
          |""".stripMargin

      withTempFile("multi_fail_", txt) { path =>
        val out = FileValidator.validateFile(path.toString, rules)
        assert(out.isInstanceOf[FileValidationOutcome.Rejected])
        val reasons = out.asInstanceOf[FileValidationOutcome.Rejected].reasons
        assert(reasons.exists(_.contains("Missing header control 'UHDR YYYYDDD'")))
        assert(reasons.exists(_.startsWith("Bad trailer format")))
      }
    }
  }
}
