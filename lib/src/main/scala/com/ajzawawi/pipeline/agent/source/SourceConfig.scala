package com.ajzawawi.pipeline.agent.source

import com.ajzawawi.pipeline.agent.validator.file.FileRules
import com.ajzawawi.pipeline.agent.validator.row._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import java.nio.file.{Path, Paths}

final case class Concurrency(threads: Int = 4, queueSize: Int = 1024)

final case class ArchivePaths(
                               accepted: Path,
                               rejected: Path,
                               failed:   Path
                             )

final case class OutputPaths(
                              clean: Path,
                              reject: Path,
                              rowErrors: Path,
                              header: Option[Seq[String]] = None
                            )

final case class ValidatorCfg(
                               file: FileRules,
                               row: RowRules
                             )

final case class SourceConfig(
                               name: String,
                               inbox: Path,
                               output: OutputPaths,
                               archive: ArchivePaths,
                               validator: ValidatorCfg,
                               allowedExtensions: Set[String] = Set(".txt", ".TXT", ".csv", ".CSV"),
                               concurrency: Option[Concurrency] = None
                             )

object SourceConfigLoader {
  implicit val pathReader: ValueReader[Path] = (cfg: Config, path: String) => Paths.get(cfg.as[String](path))

  implicit val charReader: ValueReader[Char] = (cfg: Config, path: String) => {
    val s = cfg.as[String](path)
    require(s.length == 1, s"Expected a single character at '$path', got: '$s'")
    s.head
  }

  implicit val signRuleReader: ValueReader[SignRule] = (cfg: Config, path: String) => SignRule.from(cfg.as[String](path))

  implicit val columnRuleReader: ValueReader[ColumnRule] =
    ValueReader.relative { c: Config =>
      c.getString("kind").toLowerCase match {
        case "string" => c.as[StringRule]
        case "date" => c.as[DateRule]
        case "decimal" => c.as[DecimalRule]
        case other => throw new IllegalArgumentException(s"Unknown column kind: $other")
      }
    }

  def loadAll(): List[SourceConfig] =
    ConfigFactory.load().as[List[SourceConfig]]("pipelineAgent.sources")
}
