package job

import java.io.{FileWriter, BufferedWriter, File}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}

/**
  * Created by arjunpuri on 1/29/17.
  */

object CrawlSink {

  /** Takes a className and turns it into an instance **/
  def apply(className: String): CrawlSink = {
    Class.forName(className).newInstance().asInstanceOf[CrawlSink]
  }
}

trait CrawlSink {

  /**
    * Writes a batch of futures to JSON
    *
    * @param items   items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: Future[Seq[String]], partNum: Int, outputDir: File)(implicit executionContext: ExecutionContext): Unit

}

class FileSink extends CrawlSink {

  override def writeItems(
    items: Future[Seq[String]], partNum: Int, outputDir: File)(implicit executionContext: ExecutionContext): Unit = {
    val tsDir= new File(outputDir, s"${System.currentTimeMillis()/1000}")
    val fileToWrite = new File(tsDir, s"/part-$partNum.txt")
    fileToWrite.getParentFile.mkdirs()
    fileToWrite.createNewFile()
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(items.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
  }

}

