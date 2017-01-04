package job

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{StandardOpenOption, Paths}

import client.{HNItem, HNClient}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.rogach.scallop._


/**
  * Created by arjunpuri on 11/29/16.
  */

object HNCrawler {

//  def main(args: Array[String]): Unit = {
//    new HNCrawler().runJob()
//  }

  implicit class CSVWrapper(val prod: Product) extends AnyVal {
    def toCSV(delim: String) = prod.productIterator.map{
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(delim)
  }


}

class HNCrawler(outputDir: File, maxId: Option[Long] = None, client: HNClient, batchSize: Int = 10000) {

  import HNCrawler._

  private val DELIMITER = "|"
  private val TIMEOUT = 10.second

  def runJob(): Unit = {
    val maxItemId = Await.result(client.maxItemId(), TIMEOUT).toLong
    var i = maxId.getOrElse(maxItemId)
    var partNum = 0
    println(s"Found max Id as: $i")
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - batchSize)
      crawlItems(i, lowerId, partNum)
      i = lowerId
      partNum += 1
    }
    client.shutdown()
  }

  /**
    * For a range of item ids, fires off async API requests to
    * retrieve item JSON.
    *
    * @param idHi: Upper bound of ids to crawl
    * @param idLo: Lower bound of ids to crawl
    * @param partNum: Essentially a batch number
    */
  def crawlItems(idHi: Long, idLo: Long, partNum: Int): Unit = {
    val items = (idHi to idLo by -1).map(client.item)
    writeItems(items, partNum)
  }

  /**
    * Writes a batch of futures to CSV
    *
    * @param items items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: Seq[Future[HNItem]], partNum: Int): Unit = {
    val csvToWrite = items.map(_.map(_.toCSV(DELIMITER)))
    val seqFuture = Future.sequence(csvToWrite).map(_.mkString("\n"))
    val fileToWrite = new File(outputDir, s"/part-$partNum.txt")
    fileToWrite.getParentFile.mkdirs()
    fileToWrite.createNewFile()
    val fileChannel = AsynchronousFileChannel.open(Paths.get(fileToWrite.getAbsolutePath), StandardOpenOption.WRITE)
    val byteFuture = seqFuture.map(itemStr => ByteBuffer.wrap(itemStr.getBytes))
    byteFuture.foreach(bytes => fileChannel.write(bytes, 0))
    Await.result(byteFuture, Duration.Inf)
  }


}
