package com.ajzawawi.pipeline.agent.validator.row.checks

import com.ajzawawi.pipeline.agent.validator.row.SignRule
import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._

object DecimalChecks {
  def decimal(index: Int): RowCheck =
    onCol(index) { (v, ctx) =>
      if (v.trim.isEmpty) Nil
      else if (parseBigDecimal(v).isEmpty) List(err(ctx, Some(index), "BAD_DECIMAL", "Not a valid decimal"))
      else Nil
    }

  def decimalMin(index: Int, min: BigDecimal): RowCheck =
    onCol(index) { (v, ctx) =>
      parseBigDecimal(v).filter(_ < min).map(_ => err(ctx, Some(index), "MIN", s"< $min")).toList
    }

  def decimalMax(index: Int, max: BigDecimal): RowCheck =
    onCol(index) { (v, ctx) =>
      parseBigDecimal(v).filter(_ > max).map(_ => err(ctx, Some(index), "MAX", s"> $max")).toList
    }

  def decimalSign(index: Int, sign: SignRule): RowCheck =
    onCol(index) { (v, ctx) =>
      parseBigDecimal(v).flatMap { bd =>
        sign match {
          case SignRule.Positive if bd <= 0 => Some(err(ctx, Some(index), "NOT_POSITIVE", "Must be > 0"))
          case SignRule.Negative if bd >= 0 => Some(err(ctx, Some(index), "NOT_NEGATIVE", "Must be < 0"))
          case _                            => None
        }
      }.toList
    }
}