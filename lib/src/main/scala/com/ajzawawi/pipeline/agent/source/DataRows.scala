package com.ajzawawi.pipeline.agent.source

object DataRows {
  /** Assumes file-level checks enforced:
   * - 1st non-empty: header control
   * - 2nd non-empty: column header row
   * - last non-empty: trailer line
   */
  def extract(lines: List[String]): List[String] = {
    val ne = lines.iterator.map(_.trim).filter(_.nonEmpty).toList
    if (ne.length >= 3) ne.slice(2, ne.length - 1) else Nil
  }
}