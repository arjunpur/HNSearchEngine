package crawler

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import spray.json._
import client.{HNItem, HNClient, HNJsonProtocol}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.rogach.scallop._

import scala.util.{Try, Failure, Success}


/**
  * Created by arjunpuri on 11/29/16.
  */


/**
  * Arguments for the crawler
  */
class HNCrawlerArgs(args: Seq[String]) extends ScallopConf(args) {
  val maxId = opt[Long](name = "maxId")
  val batchSize = opt[Int](name = "batchSize", default = Some(10000))
  /* For local, outputs part files here. For S3, uses it as temp store */
  val outputDir = opt[File](name = "outputDir", default = Some(CrawlSink.DEFAULT_OUTPUT_DIR))
  val bucketName = opt[String](name = "bucketName", default = Some(S3CrawlSink.DEFAULT_S3_BUCKET_NAME))
  val sinkType = opt[String](name = "sinkType", default = Some(CrawlSink.DEFAULT_SINK))
  verify()
}

object HNCrawler {

  def main(args: Array[String]): Unit = {
    val arguments = new HNCrawlerArgs(args)
    val client = new HNClient()
    val sink = CrawlSink(arguments.outputDir.apply(), arguments.bucketName.apply(), arguments.sinkType.apply())
    val crawler = new HNCrawler(arguments.maxId.toOption, client, arguments.batchSize.apply(), sink)
    crawler.init()
    crawler.runJob()
  }

}

case class CrawlState(timestamp: Long)

/**
  * @param maxId maximum id to crawl
  * @param client client to communicate with the HN api
  * @param batchSize how many items to write in a single batch
  * @param sink where to write the crawled data
  */
class HNCrawler(maxId: Option[Long] = None, client: HNClient, batchSize: Int = 10000, sink: CrawlSink) extends StrictLogging {

  import HNJsonProtocol._

  def init(): Unit = {
    sink.init()
  }

  /** Runs the crawl **/
  def runJob(): Unit = {
    val crawlState = CrawlState(System.currentTimeMillis())
    val maxItemId = Await.result(client.maxItemId(), Duration.Inf).toLong // Find max available item id
    var i = maxId.getOrElse(maxItemId)
    var partNum = 0
    logger.info(s"Found max Id as: $i")
    val currentTime = System.currentTimeMillis()
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - batchSize)
      val idsToProcess = i to lowerId by -1

      logger.info(s"Processing Ids: ${idsToProcess.min} to ${idsToProcess.max}")
      val (items, failures) = crawlItems(idsToProcess)
      logger.info(s"Got back ${items.size} success items")
      logger.info(s"Got back ${failures.size} failed items: $failures")

      val serializedItems = items.map(_.toJson.toString) // api call foreach id, then serialize
      sink.writeItems(serializedItems, partNum, crawlState)
      i = lowerId
      partNum += 1
    }
    sink.complete(crawlState)
    logger.info(s"Crawl duration: ${System.currentTimeMillis() - currentTime} ms. Outputted to ${new File(sink.outputDir, crawlState.timestamp.toString)}")
    client.shutdown()
  }

  /** Crawls a series of ids an syncrhonously writes to sink */
  def crawlItems(idsToCrawl: Seq[Long]): (Seq[HNItem], Seq[String]) = {
    val crawledFutures = idsToCrawl.map(client.item)
    val triedItems = crawledFutures.map(futureToFutureTry)
    val results = Await.result(Future.sequence(triedItems), Duration.Inf).zip(idsToCrawl)
    val successItems = results.map(_._1).flatMap(_.toOption)
    val failedItems = results.collect {
      case (Failure(e), idx) => s"Item #$idx failed with: ${e.getLocalizedMessage}"
    }
    (successItems, failedItems)
  }

  /** Converts a future to a try future */
  private def futureToFutureTry[T](f: Future[T]): Future[Try[T]] = {
    f.map(Success(_)) .recover({case x => Failure(x)})
  }

}
