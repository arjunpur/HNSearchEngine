package index

import java.io.File
import java.nio.file.Path

import crawler.CrawlSink
import org.rogach.scallop.ScallopConf

/**
  * Created by arjunpuri on 3/26/17.
  */

trait Source {


}

object Source {

  val DEFAULT_SOURCE = "index.LocalSource"

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(inputDir: Path, sourceType: String): Source = {
    val source = sourceType match {
      case "index.S3CrawlSource" => LocalSource(inputDir)
      case "index.LocalCrawlSource" =>
      case _ => throw new IllegalArgumentException(s"$sourceType does not exist")
    }
  }
}

case class LocalSource(inputDir: Path) {

}


class BulkIndexerArgs(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[Path](name = "input", default = Some(CrawlSink.DEFAULT_OUTPUT_DIR.toPath))
  val sourceType = opt[String](name = "sourceType", default = Some(Source.DEFAULT_SOURCE))
  verify()
}

/**
  * Reads a directory of serialized [[client.HNItem]] and indexes them into the
  * elasticsearch cluster
  */
object BulkIndexerJob {

  def main(args: Array[String]) = {
    val indexArgs = new BulkIndexerArgs(args)
    val source = Source(indexArgs.input.apply(), indexArgs.sourceType.apply())

  }



}
