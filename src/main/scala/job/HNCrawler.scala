package job

import java.io.{FileWriter, BufferedWriter, File}

import org.scalactic.ErrorMessage
import spray.json._
import client.{HNItem, HNClient, HNJsonProtocol}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.rogach.scallop._



/**
  * Created by arjunpuri on 11/29/16.
  */

object HNCrawler {

  def main(args: Array[String]): Unit = {
    val arguments = new HNCrawlerArgs(args)
    val crawler = new HNCrawler(
      arguments.outputDir.apply(),
      arguments.maxId.toOption,
      new HNClient(),
      arguments.batchSize.apply()
    )
    crawler.init()
    crawler.runJob()
  }

}

/**
  * Arguments for the crawler
  */
class HNCrawlerArgs(args: Seq[String]) extends ScallopConf(args) {
  val outputDir = opt[File](default = Some(new File("/Users/arjunpuri/tmp/crawler")))
  val maxId = opt[Long](default = Some(1000))
  val batchSize = opt[Int](default = Some(10000))
  verify()
}

class HNCrawler(outputDir: File, maxId: Option[Long] = None, client: HNClient, batchSize: Int = 10000) {

  import HNJsonProtocol._

  private val DELIMITER = "|"
  private val TIMEOUT = 100.second

  def init(): Unit = {
    if (!outputDir.exists()) outputDir.mkdirs()
  }

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
    val items = (idHi to idLo by -1).map(client.item).toSeq
    writeItems(items, partNum)
  }

  /**
    * Writes a batch of futures to CSV
    *
    * @param items items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: Seq[Future[HNItem]], partNum: Int): Unit = {
    val errors = Seq.newBuilder[ErrorMessage]
    val jsonToWrite = items.map(_.map(item => Some(item.toJson.toString()))
      .recover { case e => errors += e.getMessage; None })
    val seqFuture = Future.sequence(jsonToWrite).map(_.flatten)
    val fileToWrite = new File(outputDir, s"/part-$partNum.txt")
    fileToWrite.getParentFile.mkdirs()
    fileToWrite.createNewFile()
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(seqFuture.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
    println("done")
  }

}
