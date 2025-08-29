package com.ajzawawi.pipeline.agent.validator.file

case class ValidationResult(ok: Boolean, reason: Option[String])

sealed trait FileValidationOutcome
object FileValidationOutcome {
  case class Accepted(dataRowCount: Int) extends FileValidationOutcome
  case class Rejected(reasons: List[String]) extends FileValidationOutcome
}