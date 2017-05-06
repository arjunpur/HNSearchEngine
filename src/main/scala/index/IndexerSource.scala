package index

import java.io.File
import java.nio.file.Path
import client.HNItem
import com.typesafe.scalalogging.StrictLogging
import spray.json._
import client.HNJsonProtocol._

import scala.io.Source
import scala.util.Try

/**
  * Created by arjunpuri on 3/26/17.
  */

trait IndexerSource extends StrictLogging {

  /** Streams crawled output for the consumer to process **/
  def read(input: Path): Iterator[HNItem]

}

object IndexerSource {

  val DEFAULT_SOURCE = "index.LocalSource"

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(sourceType: String): IndexerSource = {
    sourceType match {
      case "index.S3Source" => S3Source()
      case "index.LocalSource" => LocalSource()
      case _ => throw new IllegalArgumentException(s"$sourceType does not exist")
    }
  }

}

case class LocalSource() extends IndexerSource {

  override def read(input: Path): Iterator[HNItem] = {
    logger.info(s"Reading files from ${input.toFile.getName}")
    val subFiles = input.toFile.listFiles()
    val isTsDir = subFiles.map(_.getName).contains("done.txt")
    /* If pointing to a timestamp directory, get an iterator of all files.
     * Else find the largest tsDir and iterate through those */
    if (isTsDir) {
      val partFiles = subFiles.filter(_.getName.startsWith("part")).toIterator
      partFiles.flatMap(file => deserializeFile(file))
    } else {
      logger.info("Looking for max tsDir...")
      val maxTsDir = subFiles.map(_.getName.toLong).max
      val tsDir = input.resolve(maxTsDir.toString)
      /* Avoid infinite recursion by throwing exception before recursing */
      if (!tsDir.toFile.listFiles().map(_.getName).contains("done.txt")) {
        throw new IllegalArgumentException(s"Invalid input path: $input. No done.txt found in tsDir: $tsDir")
      }
      logger.info(s"Found tsDir: $maxTsDir. Reading files from here...")
      read(tsDir)
    }
  }

  private def deserializeFile(file: File): Iterator[HNItem] = {
    Source.fromFile(file).getLines().flatMap(str => Try(str.parseJson.convertTo[HNItem]).toOption)
  }

}

case class S3Source() extends IndexerSource {
  override def read(input: Path): Iterator[HNItem] = ???
}

