package com.ajzawawi.pipeline.agent.validator.row

sealed trait ColumnRule {
  def index: Int
  def notBlank: Boolean
}

sealed trait SignRule
object SignRule {
  case object Positive extends SignRule
  case object Negative extends SignRule
  case object Any extends SignRule

  def from(s: String): SignRule = s.toLowerCase match {
    case "positive" => Positive
    case "negative" => Negative
    case _ => Any
  }
}

final case class StringRule(
                             index: Int,
                             notBlank: Boolean = false,
                             minLen: Option[Int] = None,
                             maxLen: Option[Int] = None,
                             regex: Option[String] = None     // an optional pattern the string must match
                           ) extends ColumnRule

final case class DateRule(
                           index: Int,
                           notBlank: Boolean = false,
                           dateFormat: String = "yyyy-MM-dd",
                           minDate: Option[String] = None,  // same format as dateFormat
                           maxDate: Option[String] = None
                         ) extends ColumnRule

final case class DecimalRule(
                              index: Int,
                              notBlank: Boolean = false,
                              sign: SignRule = SignRule.Any,
                              min: Option[BigDecimal] = None,
                              max: Option[BigDecimal] = None,
                            ) extends ColumnRule

final case class RowRules(
                           delimiter: Char = ',',
                           expectedCols: Option[Int] = None,
                           columns: List[ColumnRule] = Nil
                         )
