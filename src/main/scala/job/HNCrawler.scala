package job

import java.io.File

import client.{HNItem, HNClient}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

import purecsv.unsafe._

/**
  * Created by arjunpuri on 11/29/16.
  */

object HNCrawler {

  def main(args: Array[String]): Unit = {
    new HNCrawler().runJob()
  }

}

class HNCrawler {

  private val BATCH_SIZE = 1000
  private val DELIMITER = "|"
  private val CSV_FILE = "/Users/arjunpuri/tmp/"
  private val TIMEOUT = 10.second
  private val client = new HNClient()

  def runJob(): Unit = {
    val maxItemId = Await.result(client.maxItemId(), TIMEOUT).toLong
    var i = maxItemId
    var partNum = 0
    println(s"Found max Id as: $i")
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - BATCH_SIZE)
      crawlItems(i, lowerId, partNum)
      i = lowerId
      partNum += 1
    }
  }

  /**
    * For a range of item ids, fires off async API requests to
    * retrieve item JSON.
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
    * @param items items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: IndexedSeq[Future[HNItem]], partNum: Int): Unit = {
    val csvToWrite = items.map(_.map(_.toCSV(DELIMITER)))
    val seqFuture = Future.sequence(csvToWrite)
    seqFuture.foreach(_.writeCSVToFile(new File(CSV_FILE, s"part-$partNum"), sep = DELIMITER))
  }


}
