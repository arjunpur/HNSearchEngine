package job

import java.io.{FileWriter, BufferedWriter, File}

import com.typesafe.scalalogging.StrictLogging
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


/**
  * Arguments for the crawler
  */
class HNCrawlerArgs(args: Seq[String]) extends ScallopConf(args) {
  val outputDir = opt[File](default = Some(new File("/Users/arjunpuri/tmp/crawler")))
  val maxId = opt[Long](default = Some(1000))
  val batchSize = opt[Int](default = Some(10000))
  verify()
}

object HNCrawler {

  private val TIMEOUT = 100.second

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

trait CrawlSink {

  def write(items: Future[Seq[String]]): Unit {

  }
}

class HNCrawler(outputDir: File, maxId: Option[Long] = None, client: HNClient, batchSize: Int = 10000) extends StrictLogging {

  import HNJsonProtocol._

  def init(): Unit = {
    if (!outputDir.exists()) outputDir.mkdirs()
  }

  def runJob(): Unit = {
    val maxItemId = Await.result(client.maxItemId(), HNCrawler.TIMEOUT).toLong // Find max available item id
    var i = maxId.getOrElse(maxItemId)
    var partNum = 0
    logger.info(s"Found max Id as: $i")
    val currentTime = System.currentTimeMillis()
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - batchSize)
      crawlItems(i, lowerId, partNum)
      i = lowerId
      partNum += 1
    }
    logger.info(s"Crawl duration: ${System.currentTimeMillis() - currentTime}")
    client.shutdown()
  }

  /**
    * For a range of item ids, fires off async API requests to
    * retrieve item JSON.
    * @param idHi    : Upper bound of ids to crawl
    * @param idLo    : Lower bound of ids to crawl
    * @param partNum : Essentially a batch number
    */
  def crawlItems(idHi: Long, idLo: Long, partNum: Int): Unit = {
    val items = (idHi to idLo by -1).map(client.item).toSeq
    val serializedItems = serializeItems(items)
    writeItems(serializedItems, partNum)
  }

  /** Serializes a sequence of [[HNItem]] **/
  private def serializeItems(items: Seq[Future[HNItem]]): Future[Seq[String]] = {
    val errors = Seq.newBuilder[ErrorMessage]
    val jsonToWrite = items.map(_.map(item => Some(item.toJson.toString()))
      .recover { case e => errors += e.getMessage; None })
    logger.error(s"Following serialization errors: ${errors.result().size}")
    logger.info(s"Writing ${jsonToWrite.size} items")
    Future.sequence(jsonToWrite).map(_.flatten)
  }

  /**
    * Writes a batch of futures to CSV
    * @param items   items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: Future[Seq[String]], partNum: Int): Unit = {
    val fileToWrite = new File(outputDir, s"/part-$partNum.txt")
    fileToWrite.getParentFile.mkdirs()
    fileToWrite.createNewFile()
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(items.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
  }

}
