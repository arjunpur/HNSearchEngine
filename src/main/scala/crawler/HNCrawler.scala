package crawler

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
trait CrawlerArgs extends ScallopConf {
  val maxId = opt[Long](default = Some(1000))
  val batchSize = opt[Int](default = Some(10000))
}

object HNCrawler {

  def main(args: Array[String]): Unit = {
    val arguments = new ScallopConf(args) with CrawlerArgs with CrawlSinkArgs {()}
    arguments.verify()
    val client = new HNClient()
    val sink = CrawlSink(arguments)
    val crawler = new HNCrawler(arguments.maxId.toOption, client, arguments.batchSize.apply(), sink)
    crawler.init()
    crawler.runJob()
  }

}

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
    val maxItemId = Await.result(client.maxItemId(), Duration.Inf).toLong // Find max available item id
    var i = maxId.getOrElse(maxItemId)
    var partNum = 0
    logger.info(s"Found max Id as: $i")
    val currentTime = System.currentTimeMillis()
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - batchSize)
      val items = serializeItems((i to lowerId by -1).map(client.item).toSeq) // api call foreach id, then serialize
      sink.writeItems(items, partNum)
      i = lowerId
      partNum += 1
    }
    logger.info(s"Crawl duration: ${System.currentTimeMillis() - currentTime}")
    client.shutdown()
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

}
