package com.ajzawawi.pipeline.agent.validator.row.checks

import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._

import scala.util.matching.Regex

object StringChecks {
  def minLen(index: Int, n: Int): RowCheck =
    onCol(index) { (v, ctx) =>
      val t = v.trim
      if (t.nonEmpty && t.length < n) List(err(ctx, Some(index), "MIN_LEN", s"Length < $n")) else Nil
    }

  def maxLen(index: Int, n: Int): RowCheck =
    onCol(index) { (v, ctx) =>
      val t = v.trim
      if (t.length > n) List(err(ctx, Some(index), "MAX_LEN", s"Length > $n")) else Nil
    }

  def matches(index: Int, pattern: String): RowCheck = {
    val rx: Regex = pattern.r
    onCol(index) { (v, ctx) =>
      val t = v.trim
      if (t.nonEmpty && !rx.pattern.matcher(t).matches())
        List(err(ctx, Some(index), "REGEX", s"Does not match /$pattern/"))
      else Nil
    }
  }
}