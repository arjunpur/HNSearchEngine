package crawler

import java.io.{PrintWriter, FileWriter, BufferedWriter, File}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import spray.json._
import spray.json.DefaultJsonProtocol

import scala.io.Source
import com.amazonaws.services.s3.model.S3ObjectInputStream

/**
  * Created by arjunpuri on 1/29/17.
  */

object CrawlState extends DefaultJsonProtocol {
  implicit val post = jsonFormat2(CrawlState.apply)
}

/**
 * Holds any state that the crawler needs to persist to aid with
 * future crawls
 */
case class CrawlState(timestamp: Long, lastCrawledId: Long = 0L)

object CrawlSink {

  /* Default crawl sink to generate */
  val DEFAULT_SINK = "crawler.LocalCrawlSink"

  /* Default location to store temp part file */
  val DEFAULT_OUTPUT_DIR: File = new File(s"${System.getProperty("user.home")}/tmp/crawler")

  /** Using a type reference to the crawler sink, generate the appropriate sink */
  def apply(outputDir: File, bucketName: String, sinkType: String): CrawlSink = {
    val crawlSink = sinkType match {
      case "crawler.S3CrawlSink" => S3CrawlSink(outputDir, bucketName)
      case "crawler.LocalCrawlSink" => LocalCrawlSink(outputDir)
      case _ => throw new IllegalArgumentException(s"$sinkType does not exist")
    }
    crawlSink.init()
    crawlSink
  }

}

/**
  * Trait which any crawler sink will implement to write out items
  */
sealed trait CrawlSink extends StrictLogging { 

  /**
   * Returns the location of crawled output
   */ 
  def outputDir: File

  /**
   * Any init code
   */
  def init(): Unit = {}

  /**
   * Writes a done.txt in the output location to indicate a completed crawl
   */
  def complete(crawlState: CrawlState): Unit = {
    val tsDir = new File(outputDir, crawlState.timestamp.toString)
    new File(tsDir, "done.txt").createNewFile()
  }

  /**
   * Implementations define this function
   */
  def writeItems(items: Seq[String], partNum: Int, crawlState: CrawlState)(implicit executionContext: ExecutionContext): Unit
  
  /**
   * Returns the [[CrawlState]] in the sink
   */
  def getCrawlState(): CrawlState

  /**
   * Persists [[CrawlState]] to sink
   */
  def persistCrawlState(crawlState: CrawlState): Unit = {
    val writer = new PrintWriter(new File(outputDir, getCrawlStateName))
    writer.write(crawlState.toJson.toString)
    writer.close()
  }
  
  /**
   * Gets the name of the crawl state
   */
  protected def getCrawlStateName(): String = ".crawl_state"

  /**
   * Gets the name of a part file given its number. A part file is just an output partition
   */
  protected def getPartFileName(partNum: Int): String = f"/part-${partNum}%05d"

  /**
    * Writes items to intermediary file
    *
    * @return Returns the timestamp directory to which the intermediary files were written
    */
  protected def writeToIntermediary(items: Seq[String], partNum: Int, crawlState: CrawlState, outputDir: File)(implicit executionContext: ExecutionContext): File = {
    val tsDir = new File(outputDir, crawlState.timestamp.toString)
    tsDir.mkdirs()
    /* Create file to write and write to it */
    val fileToWrite = new File(tsDir, getPartFileName(partNum))
    val writer = new BufferedWriter(new FileWriter(fileToWrite))
    items.foreach(json => {
      writer.write(json)
      writer.newLine()
    })
    writer.close()
    tsDir
  }

}


/** Sink to write to local disk **/
case class LocalCrawlSink(outputDir: File) extends CrawlSink with StrictLogging {

  override def init(): Unit = {
    if (!outputDir.exists()) outputDir.mkdirs()
  }

  override def writeItems(items: Seq[String], partNum: Int, crawlState: CrawlState)(implicit executionContext: ExecutionContext): Unit = {
    writeToIntermediary(items, partNum, crawlState, outputDir)
  }

  override def getCrawlState(): CrawlState = {
    val crawlState = new File(outputDir, getCrawlStateName())
    if (!crawlState.exists()) CrawlState(System.currentTimeMillis())
    else Source.fromFile(crawlState).mkString.parseJson.convertTo[CrawlState]
  }

}

object S3CrawlSink {
  /* Default S3 bucket name */
  val DEFAULT_S3_BUCKET_NAME: String = "hn-crawl"
  /* Number of retries on writing to S3 */
  val NUM_RETRIES = 3
}


/** S3 sink to write out items to S3 */
case class S3CrawlSink(outputDir: File, bucketName: String) extends CrawlSink with StrictLogging {

  val s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build()

  override def init(): Unit = {
    if (!outputDir.exists()) outputDir.mkdirs()
  }

  /** Writes a batch of futures to JSON
    *
    * @param items   items to write
    * @param partNum batch number of writes to distinguish file names
    */
  override def writeItems(items: Seq[String], partNum: Int, crawlState: CrawlState)(implicit executionContext: ExecutionContext): Unit = {
    /* Upload file to S3 and delete */
    val tsDir = writeToIntermediary(items, partNum, crawlState, outputDir)
    val fileToWrite = new File(tsDir, getPartFileName(partNum))
    retry(S3CrawlSink.NUM_RETRIES)(s3Client.putObject(bucketName, s"${crawlState.timestamp.toString}/${fileToWrite.getName}", fileToWrite))
    fileToWrite.delete()
  }

  override def complete(crawlState: CrawlState): Unit = {
    super.complete(crawlState)
    s3Client.putObject(bucketName, s"${crawlState.timestamp}/done.txt", new File("done.txt"))
  }

  override def persistCrawlState(crawlState: CrawlState): Unit = {
    super.persistCrawlState(crawlState)
    s3Client.putObject(bucketName, s"${getCrawlStateName()}", new File(outputDir, getCrawlStateName))
  }

  override def getCrawlState(): CrawlState = {
    if (s3Client.doesObjectExist(bucketName, getCrawlStateName)) {
      val objectContent: S3ObjectInputStream = s3Client.getObject(bucketName, getCrawlStateName).getObjectContent()
      val crawlState: CrawlState = Source.fromInputStream(objectContent).mkString.parseJson.convertTo[CrawlState]
      crawlState
    } else {
      CrawlState(System.currentTimeMillis())
    }
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

