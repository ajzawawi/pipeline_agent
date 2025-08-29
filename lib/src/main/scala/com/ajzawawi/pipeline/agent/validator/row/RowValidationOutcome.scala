package com.ajzawawi.pipeline.agent.validator.row

import scala.collection.immutable.Vector

final case class RowError(rowNum: Int, colIdx: Option[Int], code: String, message: String, rawRow: String)

sealed trait RowValidationOutcome
object RowValidationOutcome {
  final case class Valid(rowNum: Int, cells: Vector[String]) extends RowValidationOutcome
  final case class Invalid(rowNum: Int, cells: Vector[String], errors: List[RowError]) extends RowValidationOutcome
}