package com.ajzawawi.pipeline.agent.validator.row.checks

import com.ajzawawi.pipeline.agent.validator.row.RowError

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

object ChecksSupport {
  // A row-level check will returns zero or more errors
  type RowCheck = (Vector[String], RowCtx) => List[RowError]

  final case class RowCtx(rowNum: Int, rawLine: String)

  def err(ctx: RowCtx, idx: Option[Int], code: String, msg: String): RowError =
    RowError(ctx.rowNum, idx, code, msg, ctx.rawLine)

  def onCol(index: Int)(f: (String, RowCtx) => List[RowError]): RowCheck =
    (cells, ctx) =>
      if (index >= 0 && index < cells.length) f(cells(index), ctx)
      else List(err(ctx, Some(index), "MISSING_COL", s"Column $index missing in row"))

  def parseDate(fmt: DateTimeFormatter)(s: String): Option[LocalDate] =
    Try(LocalDate.parse(s.trim, fmt)).toOption

  def parseBigDecimal(s: String): Option[BigDecimal] =
    Try(BigDecimal(s.trim)).toOption
}
