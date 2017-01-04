package job

import java.io.File

import client.{HNClient, HNItem}
import org.scalatest.FunSuite

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
  * Created by arjunpuri on 12/26/16.
  */
class HNCrawlerTest extends FunSuite{

  test("Crawler writes a sequence of items to a file asyncrhonously") {
    val outputDir = new File("/Users/arjunpuri/tmp/crawler")
    val crawler = new HNCrawler(outputDir, None, new HNClient(), batchSize = 1)
    val items = Seq(
      Future(HNItem(id = 1, text = Some("Item 1"))),
      Future(HNItem(id = 2, text = Some("Item 2")))
      )
    crawler.writeItems(items, 1)
    val text = new File(outputDir, "part-1.txt")
    assert(Source.fromFile(text).mkString === "1|||||Item 1||||||||\n2|||||Item 2||||||||")
  }


}
