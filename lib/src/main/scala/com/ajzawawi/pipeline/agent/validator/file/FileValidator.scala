package com.ajzawawi.pipeline.agent.validator.file

import scala.io.Source
import scala.util.Using

object FileValidator {
  /** Runs ALL validations and returns Accepted/Rejected.
   * On Accepted, dataRowCount = non-empty lines minus (UHDR + header + trailer).
   * On Rejected, reasons = list of each failed check’s message (in build order).
   */
  def validateFile(path: String, rules: FileRules = FileRules()): FileValidationOutcome = {
    val lines: List[String] = Using.resource(Source.fromFile(path)("UTF-8")) { src =>
      src.getLines().toList
    }
    val checks  = FileLevelChecks.build(rules)
    val results: List[ValidationResult] = checks.map(_(lines))
    val failures: List[String] = results.collect { case ValidationResult(false, Some(msg)) => msg }

    if (failures.nonEmpty) FileValidationOutcome.Rejected(failures)
    else {

      val nonEmpty = lines.iterator.map(_.trim).filter(_.nonEmpty).toList

      // UHDR + header row + trailer
      val dataRowCount = math.max(0, nonEmpty.size - 3)
      FileValidationOutcome.Accepted(dataRowCount)
    }
  }
}