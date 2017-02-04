package job

import java.io.{FileWriter, BufferedWriter, File}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by arjunpuri on 1/29/17.
  */

object CrawlSink {

  /* Default location to store temp part file */
  val DEFAULT_OUTPUT_DIR: File = new File(s"${System.getProperty("user.home")}/tmp/crawler")
  /* Default S3 bucket name */
  val DEFAULT_S3_BUCKET_NAME: String = "hn-crawl"
  /* Number of retries on writing to S3 */
  val NUM_RETRIES = 3

}

class CrawlSink(outputDir: File, s3BucketName: String) extends StrictLogging {

  def init(): Unit = {
    if (!outputDir.exists()) outputDir.mkdirs()
    outputDir.deleteOnExit()
  }

  /** Writes a batch of futures to JSON
    * @param items   items to write
    * @param partNum batch number of writes to distinguish file names
    */
  def writeItems(items: Future[Seq[String]], partNum: Int)(implicit executionContext: ExecutionContext): Unit = {
    val tsDir = new File(outputDir, s"${System.currentTimeMillis()/1000}")
    tsDir.mkdirs()
    /* Create file to write and write to it */
    val fileToWrite = new File(tsDir, s"/part-$partNum")
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(items.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
    writer.close()
    /* Upload file to S3 and delete */
    retry(CrawlSink.NUM_RETRIES)(uploadToS3(fileToWrite))
    fileToWrite.delete()
  }

  /** Write file to S3 **/
  private def uploadToS3(file: File) = {
    val s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build()
    s3Client.putObject(s3BucketName, "random", file)
  }

  /** Retries running a function some number of times **/
  private def retry[A](num: Int)(fn: => A): A = {
    Try(fn) match {
      case Success(res) => res
      case _ if num > 1 => retry[A](num - 1)(fn)
      case Failure(e) => throw e
    }
  }

}

