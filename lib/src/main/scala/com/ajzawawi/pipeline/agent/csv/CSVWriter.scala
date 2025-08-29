package com.ajzawawi.pipeline.agent.csv

import java.io.{BufferedWriter, OutputStreamWriter, Writer}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.Using

object CsvWriter {
  /** Write CSV rows (and optional header) to a file.
   * - Cells are quoted when needed (delimiter, quotes, CR/LF, or leading/trailing space).
   * - Creates parent directories if missing.
   */
  def write(
             outPath: Path,
             rows: IterableOnce[Vector[String]],
             header: Option[Seq[String]] = None,
             delimiter: Char = ',',
             newline: String = "\n"
           ): Unit = {
    val parent = outPath.getParent
    if (parent != null) Files.createDirectories(parent)

    Using.resource(new BufferedWriter(new OutputStreamWriter(
      Files.newOutputStream(outPath), StandardCharsets.UTF_8))) { w =>
      header.foreach(h => writeLine(w, h.toIndexedSeq, delimiter, newline))
      val it = rows.iterator
      it.foreach(r => writeLine(w, r, delimiter, newline))
    }
  }

  private def writeLine(
                 w: Writer,
                 cells: IndexedSeq[String],
                 delimiter: Char = ',',
                 newline: String = "\n"
               ): Unit = {
    w.write(toCsvLine(cells, delimiter))
    w.write(newline)
  }

  def toCsvLine(cells: IndexedSeq[String], delimiter: Char = ','): String =
    cells.map(c => quote(c, delimiter)).mkString(delimiter.toString)

  private def quote(cell: String, delimiter: Char): String = {
    val needsQuote =
      cell.indexOf(delimiter) >= 0 ||
        cell.indexOf('"')      >= 0 ||
        cell.indexOf('\n')     >= 0 ||
        cell.indexOf('\r')     >= 0 ||
        (cell.nonEmpty && (cell.head.isWhitespace || cell.last.isWhitespace))

    if (needsQuote) "\"" + cell.replace("\"", "\"\"") + "\"" else cell
  }
}
