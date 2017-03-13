package crawler

import java.io.{FileWriter, BufferedWriter, File}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.scalalogging.StrictLogging
import org.rogach.scallop.ScallopConf

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by arjunpuri on 1/29/17.
  */

trait CrawlSinkArgs extends ScallopConf {
  /* For local, outputs part files here. For S3, uses it as temp store */
  val outputDir = opt[File](default = Some(CrawlSink.DEFAULT_OUTPUT_DIR))
  val bucketName = opt[String](default = Some(S3CrawlSink.DEFAULT_S3_BUCKET_NAME))
  val sinkType = opt[String](default = Some(CrawlSink.DEFAULT_SINK))
}

object CrawlSink {

  /* Default crawl sink to generate */
  val DEFAULT_SINK = "crawler.LocalCrawlSink"

  /* Default location to store temp part file */
  val DEFAULT_OUTPUT_DIR: File = new File(s"${System.getProperty("user.home")}/tmp/crawler")

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(args: CrawlSinkArgs): CrawlSink = {
    val crawlerClass = Class.forName(args.sinkType.apply())
    val constructor = crawlerClass.getConstructor(classOf[CrawlSinkArgs])
    constructor.newInstance(args).asInstanceOf[CrawlSink]
  }

}

/**
  * Trait which any crawler sink will implement to write out items
  */
trait CrawlSink {
  def init(): Unit = {}
  def writeItems(items: Future[Seq[String]], partNum: Int)(implicit executionContext: ExecutionContext): Unit
}

/** Sink to write to local disk **/
class LocalCrawlSink(args: CrawlSinkArgs) extends CrawlSink with StrictLogging {

  override def init(): Unit = {
    val outputDir = args.outputDir.apply()
    if (!outputDir.exists()) outputDir.mkdirs()
  }

  override def writeItems(items: Future[Seq[String]], partNum: Int)(implicit executionContext: ExecutionContext): Unit = {
    val outputDir = args.outputDir.apply()
    val timestamp = s"${System.currentTimeMillis()/1000}"
    val tsDir = new File(outputDir, timestamp)
    tsDir.mkdirs()
    /* Create file to write and write to it */
    val fileToWrite = new File(tsDir, s"/part-$partNum")
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(items.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
    writer.close()
  }

}

object S3CrawlSink {
  /* Default S3 bucket name */
  val DEFAULT_S3_BUCKET_NAME: String = "hn-crawl"
  /* Number of retries on writing to S3 */
  val NUM_RETRIES = 3
}


/** S3 sink to write out items to S3 */
class S3CrawlSink(args: CrawlSinkArgs) extends CrawlSink with StrictLogging {

  override def init(): Unit = {
    val outputDir = args.outputDir.apply()
    if (!outputDir.exists()) outputDir.mkdirs()
  }

  /** Writes a batch of futures to JSON
 *
    * @param items   items to write
    * @param partNum batch number of writes to distinguish file names
    */
  override def writeItems(items: Future[Seq[String]], partNum: Int)(implicit executionContext: ExecutionContext): Unit = {
    val outputDir = args.outputDir.apply()
    val timestamp = s"${System.currentTimeMillis()/1000}"
    val tsDir = new File(outputDir, timestamp)
    tsDir.mkdirs()
    /* Create file to write and write to it */
    val fileToWrite = new File(tsDir, f"/part-${partNum}%05d")
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    Await.result(items.map(_.foreach(json => {
      writer.write(json)
      writer.newLine()
    })), Duration.Inf)
    writer.close()
    /* Upload file to S3 and delete */
    retry(S3CrawlSink.NUM_RETRIES)(uploadToS3(s"$timestamp/${fileToWrite.getName}", fileToWrite))
    fileToWrite.delete()
  }

  /** Write file to S3 **/
  private def uploadToS3(key: String, file: File) = {
    val s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build()
    s3Client.putObject(args.bucketName.apply(), key, file)
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

