package com.ajzawawi.pipeline.agent.csv

import org.scalatest.funspec.AnyFunSpec

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

class CsvWriterSpec extends AnyFunSpec {

  private def tmpDir(): Path = Files.createTempDirectory("csvw_")
  private def read(path: Path): String = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)

  describe("CsvWriter.toCsvLine") {

    it("renders a simple row without quoting when not needed") {
      val s = CsvWriter.toCsvLine(Vector("AAPL", "USD", "100", "2025-08-26"))
      assert(s == "AAPL,USD,100,2025-08-26")
    }

    it("quotes cells containing the delimiter") {
      val s = CsvWriter.toCsvLine(Vector("a,b", "c"), delimiter = ',')
      assert(s == "\"a,b\",c")
    }

    it("escapes double quotes by doubling them and wraps in quotes") {
      val s = CsvWriter.toCsvLine(Vector("He said \"hi\""), delimiter = ',')
      assert(s == "\"He said \"\"hi\"\"\"")
    }

    it("quotes cells containing newlines or carriage returns") {
      val s1 = CsvWriter.toCsvLine(Vector("a\nb", "c"), ',')
      val s2 = CsvWriter.toCsvLine(Vector("a\rb", "c"), ',')
      assert(s1 == "\"a\nb\",c")
      assert(s2 == "\"a\rb\",c")
    }

    it("quotes cells with leading or trailing whitespace") {
      val s1 = CsvWriter.toCsvLine(Vector("  ab", "c"), ',')
      val s2 = CsvWriter.toCsvLine(Vector("ab  ", "c"), ',')
      assert(s1 == "\"  ab\",c")
      assert(s2 == "\"ab  \",c")
    }
  }

  describe("CsvWriter.write") {

    it("writes header and rows using default delimiter and newline") {
      val dir  = tmpDir()
      val out  = dir.resolve("default.csv")
      val head = Some(Seq("ENTY","CURR","QTY","DATE"))
      val rows = Seq(
        Vector("AAPL","USD","100.25","2025-08-26"),
        Vector("MSFT","USD","50","2025-08-27")
      )

      CsvWriter.write(out, rows, header = head)

      val expected =
        "ENTY,CURR,QTY,DATE\n" +
          "AAPL,USD,100.25,2025-08-26\n" +
          "MSFT,USD,50,2025-08-27\n"

      assert(read(out) == expected)
    }

    it("creates parent directories if they don't exist") {
      val dir   = tmpDir()
      val out   = dir.resolve("nested/dir/out.csv") // nested path
      val rows  = Seq(Vector("x","y"))

      CsvWriter.write(out, rows, header = Some(Seq("H1","H2")))

      assert(Files.exists(out))
      val lines = Files.readAllLines(out, StandardCharsets.UTF_8).asScala.toList
      assert(lines == List("H1,H2", "x,y"))
    }

    it("supports a custom delimiter (e.g., ';') and quotes cells containing that delimiter") {
      val dir  = tmpDir()
      val out  = dir.resolve("semi.csv")
      val head = Some(Seq("H1","H2"))
      val rows = Seq(
        Vector("a;b", "c"),     // needs quoting because of ';'
        Vector("foo", "bar")
      )

      CsvWriter.write(out, rows, header = head, delimiter = ';')

      val expected =
        "H1;H2\n" +
          "\"a;b\";c\n" +
          "foo;bar\n"

      assert(read(out) == expected)
    }

    it("supports a custom newline (e.g., CRLF)") {
      val dir  = tmpDir()
      val out  = dir.resolve("crlf.csv")
      val head = Some(Seq("H1","H2"))
      val rows = Seq(Vector("a","b"))

      CsvWriter.write(out, rows, header = head, delimiter = ',', newline = "\r\n")

      val expected = "H1,H2\r\n" + "a,b\r\n"
      assert(read(out) == expected)
    }

    it("accepts rows as an Iterator (streaming)") {
      val dir  = tmpDir()
      val out  = dir.resolve("it.csv")
      val head = Some(Seq("H1","H2"))
      val it   = Iterator(Vector("r1c1","r1c2"), Vector("r2c1","r2c2"))

      CsvWriter.write(out, it, header = head)

      val expected =
        "H1,H2\n" +
          "r1c1,r1c2\n" +
          "r2c1,r2c2\n"

      assert(read(out) == expected)
    }
  }
}
