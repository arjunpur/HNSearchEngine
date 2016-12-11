package job

import client.HNClient

import scala.concurrent.Await
import scala.concurrent.duration._

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
  private val TIMEOUT = 10.second
  private val client = new HNClient()

  def runJob(): Unit = {
    val maxItemId = Await.result(client.maxItemId(), TIMEOUT).toLong
    var i = maxItemId
    println(s"Found max Id as: $i")
    /* Batch the ids and asynchronously crawl them */
    while (i > 0) {
      val lowerId = Math.max(0, i - BATCH_SIZE)
      crawlItems(i, lowerId)
      i = lowerId
    }
  }

  /**
    * For a range of item ids, fires off async API requests to
    * retrieve item JSON
    */
  def crawlItems(idHi: Long, idLo: Long): Unit = {
    (idHi to idLo by -1).foreach(client.item)
  }

}
