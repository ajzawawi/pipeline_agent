package com.ajzawawi.pipeline.agent.validator.row.checks

import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._

object CommonChecks {
  def expectedCols(n: Int): RowCheck =
    (cells, ctx) =>
      if (cells.length == n) Nil
      else List(err(ctx, None, "BAD_COL_COUNT", s"Expected $n columns, found ${cells.length}"))

  def notBlank(index: Int): RowCheck =
    onCol(index) { (v, ctx) =>
      if (v.trim.isEmpty) List(err(ctx, Some(index), "BLANK", "Must not be blank"))
      else Nil
    }
}