package com.ajzawawi.pipeline.agent.validator.row

import org.scalatest.funspec.AnyFunSpec

class RowValidatorSpec extends AnyFunSpec {

  private val rules = RowRules(
    delimiter = ',',
    expectedCols = Some(4),
    columns = List(
      StringRule(index = 0, notBlank = true, minLen = Some(2), maxLen = Some(20)),          // ENTY
      StringRule(index = 1, notBlank = true, minLen = Some(3), maxLen = Some(3)),           // CURR
      DecimalRule(index = 2, notBlank = true, sign = SignRule.Positive, min = Some(0)),     // QTY > 0
      DateRule(index = 3, notBlank = true, dateFormat = "yyyy-MM-dd")                       // DATE
    )
  )

  describe("RowValidator.validateRow") {

    it("accepts a row that satisfies all configured rules") {
      val row = "AAPL, USD, 100.25, 2025-08-26"
      RowValidator.validateRow(row, 1, rules) match {
        case RowValidationOutcome.Valid(n, cells) =>
          assertResult(1)(n)
          assertResult(Vector("AAPL","USD","100.25","2025-08-26"))(cells)
        case other =>
          fail(s"Expected Valid, got $other")
      }
    }

    it("rejects a row with an incorrect number of columns") {
      val row = "AAPL, USD, 100.25"
      RowValidator.validateRow(row, 2, rules) match {
        case RowValidationOutcome.Invalid(n, _, errs) =>
          assertResult(2)(n)
          assert(errs.exists(_.code == "BAD_COL_COUNT"))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("rejects a row when a required field is blank") {
      val row = " , USD, 100.25, 2025-08-26"
      RowValidator.validateRow(row, 3, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(e => e.colIdx.contains(0) && e.code == "BLANK"))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("rejects a row when a field violates a fixed-length constraint") {
      val row = "AAPL, USDT, 100.25, 2025-08-26"
      RowValidator.validateRow(row, 4, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(e => e.colIdx.contains(1) && (e.code == "MIN_LEN" || e.code == "MAX_LEN")))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("rejects a row when a numeric field must be positive but is not") {
      val row = "AAPL, USD, -5, 2025-08-26"
      RowValidator.validateRow(row, 5, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(e => e.colIdx.contains(2) && e.code == "NOT_POSITIVE"))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("rejects a row when a numeric field is not parse-able") {
      val row = "AAPL, USD, X10, 2025-08-26"
      RowValidator.validateRow(row, 6, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(e => e.colIdx.contains(2) && e.code == "BAD_DECIMAL"))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("rejects a row when a date field has an invalid format") {
      val row = "AAPL, USD, 100, 08/26/2025"
      RowValidator.validateRow(row, 7, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(e => e.colIdx.contains(3) && e.code == "BAD_DATE"))
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }

    it("trims whitespace around fields prior to validation") {
      val row = "  AAPL  ,   USD ,  100  ,   2025-08-26  "
      RowValidator.validateRow(row, 8, rules) match {
        case RowValidationOutcome.Valid(_, cells) =>
          assertResult(Vector("AAPL","USD","100","2025-08-26"))(cells)
        case other =>
          fail(s"Expected Valid, got $other")
      }
    }

    it("aggregates multiple errors in one row") {
      val row = " , USDT, -5x, 2025-99-99"
      RowValidator.validateRow(row, 9, rules) match {
        case RowValidationOutcome.Invalid(_, _, errs) =>
          assert(errs.exists(_.code == "BLANK"))         // ENTY
          assert(errs.exists(e => e.colIdx.contains(1) && (e.code == "MIN_LEN" || e.code == "MAX_LEN"))) // CURR
          assert(errs.exists(_.code == "BAD_DECIMAL") || errs.exists(_.code == "NOT_POSITIVE"))          // QTY
          assert(errs.exists(_.code == "BAD_DATE"))      // DATE
          assert(errs.size >= 3)
        case other =>
          fail(s"Expected Invalid, got $other")
      }
    }
  }
}
