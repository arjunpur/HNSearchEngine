package index

import java.nio.file.Path

/**
  * Created by arjunpuri on 3/26/17.
  */

trait IndexerSource {

  val input: Path

  /** Streams crawled output for the consumer to process **/
  def read: Stream[String]

}

object IndexerSource {

  val DEFAULT_SOURCE = "index.LocalSource"

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(input: Path, sourceType: String): IndexerSource = {
    val source = sourceType match {
      case "index.S3CrawlSource" => S3Source(input)
      case "index.LocalCrawlSource" => LocalSource(input)
      case _ => throw new IllegalArgumentException(s"$sourceType does not exist")
    }
    source
  }

}

case class LocalSource(input: Path) extends IndexerSource {

  override def read: Stream[String] = {
    val subFiles = input.toFile.listFiles()
    val isTsDir = subFiles.map(_.getName).contains("done.txt")
    /* If pointing to a timestamp directory, stream all files. Else find the largest tsDir and stream */
    if (isTsDir) {
      val partFiles = subFiles.filter(_.getName.startsWith("part"))
      val readFunctions = partFiles.map(file => deserializeFile(file) _)

    } else {

    }
  }

  private def deserializeFile(file: File): String = {
    IndexerSource.fromFile(file)
  }

}

case class S3Source(input: Path) extends IndexerSource {
  override def read: Unit = ???
}

