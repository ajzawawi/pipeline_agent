package com.ajzawawi.pipeline.agent.validator.file

final case class FileRules(
                          requireHeaderRow: Boolean = true,
                          headerPrefix: String = "UHDR",
                          trailerPrefix: String = "TRL",
                          expectedColumns: Option[Seq[String]] = None,
                          columnDelimiter: Char = ','
                          )
