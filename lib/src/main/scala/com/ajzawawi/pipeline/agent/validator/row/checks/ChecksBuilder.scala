package com.ajzawawi.pipeline.agent.validator.row.checks

import StringChecks.{matches, maxLen, minLen}
import com.ajzawawi.pipeline.agent.validator.row.checks.ChecksSupport._
import com.ajzawawi.pipeline.agent.validator.row.checks.CommonChecks._
import com.ajzawawi.pipeline.agent.validator.row.checks.DateChecks._
import com.ajzawawi.pipeline.agent.validator.row.checks.DecimalChecks._
import com.ajzawawi.pipeline.agent.validator.row._

object ChecksBuilder {
  // This will take the typed ColumnRule and turn into the checks needed for that column type
  def fromRule(rule: ColumnRule): List[RowCheck] = rule match {
    case s: StringRule =>
      List.concat(
        if (s.notBlank) List(notBlank(s.index)) else Nil,
        s.minLen.map(n => minLen(s.index, n)).toList,
        s.maxLen.map(n => maxLen(s.index, n)).toList,
        s.regex.map(p => matches(s.index, p)).toList
      )

    case d: DateRule =>
      List.concat(
        if (d.notBlank) List(notBlank(d.index)) else Nil,
        List(dateFormat(d.index, d.dateFormat)),
        d.minDate.map(md => dateMin(d.index, d.dateFormat, md)).toList,
        d.maxDate.map(mx => dateMax(d.index, d.dateFormat, mx)).toList
      )

    case n: DecimalRule =>
      List.concat(
        if (n.notBlank) List(notBlank(n.index)) else Nil,
        List(decimal(n.index)),
        n.min.map(mn => decimalMin(n.index, mn)).toList,
        n.max.map(mx => decimalMax(n.index, mx)).toList,
        n.sign match {
          case SignRule.Any => Nil
          case s            => List(decimalSign(n.index, s))
        }
      )
  }
}
