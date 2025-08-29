package com.ajzawawi.pipeline.agent.validator.file
import scala.util.matching.Regex

object FileLevelChecks {
  def build(rules: FileRules): List[FileValidation] =
    List(
      requireHeaderRow(rules),
      headerControl(rules),
      trailerControl(rules)
    )

  private def nonEmpty(lines: List[String]): List[String] =
    lines.iterator.map(_.trim).filter(_.nonEmpty).toList

  // strict position: the *second* non-empty line must exist (it's the column header row)
  // and it should match the headings in the config
  private def requireHeaderRow(rules: FileRules): FileValidation = lines => {
    if (!rules.requireHeaderRow) ValidationResult(ok = true, None)
    else {
      val ne = nonEmpty(lines)

      ne.lift(1) match {
        case None =>
          ValidationResult(ok = false, Some("Missing column header row"))
        case Some(h) =>
          rules.expectedColumns match {
            case None =>
              // Presence is enough when names aren't enforced
              ValidationResult(ok = true, None)
            case Some(expected) =>
              val actual = h.split(rules.columnDelimiter).map(_.trim).toSeq
              if (actual == expected) ValidationResult(ok = true, None)
              else {
                val expStr = expected.mkString(", ")
                val actStr = actual.mkString(", ")
                ValidationResult(ok = false, Some(s"Column header missing or mismatched. Expected: $expStr; Found: $actStr"))
              }
          }
      }
    }
  }

  // strict position: the *first* non-empty line must be "<prefix> YYYYDDD"
  private def headerControl(rules: FileRules): FileValidation = lines => {
    val rx: Regex = s"^${Regex.quote(rules.headerPrefix)}\\s+\\d{7}$$".r
    val ok = nonEmpty(lines).headOption.exists(l => rx.matches(l))
    if (ok) ValidationResult(ok = true, None)
    else ValidationResult(ok = false, Some(s"Missing header control '${rules.headerPrefix} YYYYDDD'"))
  }

  // strict position: the *last* non-empty line must be "<prefix> <digits>"
  private def trailerControl(rules: FileRules): FileValidation = lines => {
    val ne = nonEmpty(lines)
    val rx: Regex = s"^${Regex.quote(rules.trailerPrefix)}\\s+(\\d+)$$".r

    ne.lastOption match {
      case None =>
        ValidationResult(ok = false, Some("Missing trailer line"))

      case Some(last) =>
        last match {
          case rx(countStr) =>
            if (ne.size < 3)
              ValidationResult(ok = false, Some("File too short for trailer validation"))
            else {
              val declared = countStr.toInt
              val actual   = ne.size - 3 // exclude header-control + header-row + trailer
              if (declared == actual) ValidationResult(ok = true, None)
              else ValidationResult(ok = false, Some(s"Trailer count $declared does not match actual $actual"))
            }

          case _ if last.startsWith(rules.trailerPrefix) =>
            ValidationResult(ok = false, Some(s"Bad trailer format. Expected '${rules.trailerPrefix} <count>'"))

          case _ =>
            ValidationResult(ok = false, Some("Missing trailer line"))
        }
    }
  }
}

