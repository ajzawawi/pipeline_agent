package com.ajzawawi.pipeline.agent.validator.row

import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._
import com.ajzawawi.pipeline.agent.validator.row.checks.CommonChecks._
import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksBuilder._

object RowValidator {
  /** Validate a single delimited data row. */
  def validateRow(line: String, rowNum: Int, rules: RowRules): RowValidationOutcome = {
    val cells = line.split(rules.delimiter).map(_.trim).toVector
    val ctx   = RowCtx(rowNum, line)

    // This builds out the checklist that will validate the rows based on their types
    val checklist: List[RowCheck] =
      rules.expectedCols.map(expectedCols).toList ++
        rules.columns.flatMap(fromRule)

    // Run all checks and collect errors
    val errors: List[RowError] = checklist.flatMap(check => check(cells, ctx))

    if (errors.isEmpty) RowValidationOutcome.Valid(rowNum, cells)
    else RowValidationOutcome.Invalid(rowNum, cells, errors)
  }
}