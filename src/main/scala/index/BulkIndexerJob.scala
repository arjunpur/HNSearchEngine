package index

import java.net.InetAddress
import java.nio.file.Path

import crawler.CrawlSink
import org.rogach.scallop.ScallopConf

/**
  * Created by arjunpuri on 3/26/17.
  */

class BulkIndexerArgs(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[Path](name = "input", default = Some(CrawlSink.DEFAULT_OUTPUT_DIR.toPath))
  val sourceType = opt[String](name = "sourceType", default = Some(IndexerSource.DEFAULT_SOURCE))
  val hosts = opt[List[String]](name = "hosts", default = Some(List(InetAddress.getLocalHost.getHostName)))
  val clusterName = opt[String](name = "clusterName", default = Some("hnsearch"))
  verify()
}

/**
  * Reads a directory of serialized [[client.HNItem]] and indexes them into the
  * elasticsearch cluster
  */
object BulkIndexerJob {

  def main(args: Array[String]) = {
    val indexArgs = new BulkIndexerArgs(args)
    val source = IndexerSource(indexArgs.sourceType.apply())
    val client = HNElasticCluster.getClient()
    BulkIndexer(client).index(source.read(indexArgs.input.apply()))
  }

}

