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

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(inputDir: File, sourceType: String): Source = {
    val source = sourceType match {
      case "crawler.S3CrawlSink" =>
      case "crawler.LocalCrawlSink" =>
      case _ => throw new IllegalArgumentException(s"$sinkType does not exist")
    }
  }
}


object LocalSource

case class LocalSource(inputDir: File) {

}


class BulkIndexerArgs(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[Path](name = "input", default = Some(CrawlSink.DEFAULT_OUTPUT_DIR.toPath))

}

/**
  * Reads a directory of serialized [[client.HNItem]] and indexes them into the
  * elasticsearch cluster
  */
object BulkIndexer {

  def main(args: Array[String]) = {


  }



}
