package com.ajzawawi.pipeline.agent.validator.row.checks

import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._

import java.time.format.DateTimeFormatter

object DateChecks {
  def dateFormat(index: Int, pattern: String): RowCheck = {
    val fmt = DateTimeFormatter.ofPattern(pattern)
    onCol(index) { (v, ctx) =>
      val t = v.trim
      if (t.isEmpty) Nil
      else if (parseDate(fmt)(t).isEmpty) List(err(ctx, Some(index), "BAD_DATE", s"Expected $pattern"))
      else Nil
    }
  }

  def dateMin(index: Int, pattern: String, minInclusive: String): RowCheck = {
    val fmt = DateTimeFormatter.ofPattern(pattern)
    val min = parseDate(fmt)(minInclusive)
    onCol(index) { (v, ctx) =>
      (for {
        d  <- parseDate(fmt)(v)
        mn <- min if d.isBefore(mn)
      } yield err(ctx, Some(index), "DATE_MIN", s"Before $minInclusive")).toList
    }
  }

  def dateMax(index: Int, pattern: String, maxInclusive: String): RowCheck = {
    val fmt = DateTimeFormatter.ofPattern(pattern)
    val max = parseDate(fmt)(maxInclusive)
    onCol(index) { (v, ctx) =>
      (for {
        d  <- parseDate(fmt)(v)
        mx <- max if d.isAfter(mx)
      } yield err(ctx, Some(index), "DATE_MAX", s"After $maxInclusive")).toList
    }
  }
}