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
  val hosts = opt[List[String]](name = "hosts", default = Some(List(InetAddress.getLocalHost.getHostName)), descr = "Host port pairs")
  val clusterName = opt[String](name = "clusterName", default = Some("hnsearch"))
  verify()
}

/**
  * Reads a directory of serialized [[client.HNItem]] and indexes them into the
  * elasticsearch cluster
  */
object BulkIndexerJob {

  def main(args: Array[String]) = {
    val indexArgs: BulkIndexerArgs = new BulkIndexerArgs(args)
    val source: IndexerSource = IndexerSource(indexArgs.sourceType.apply())
    val hostPorts: Seq[(String, Int)] = indexArgs.hosts.apply().map(host => {
      val split = host.split(":")
      if (split.size != 2) throw new IllegalArgumentException(s"$host must be a host/port pair in this format: `host:port`")
      (split.head, split.last.toInt)
    })
    val client: HNElasticCluster = HNElasticCluster(indexArgs.clusterName.apply(), hostPorts)
    val bulkIndexer: BulkIndexer = BulkIndexer(client)
    bulkIndexer.init()
    bulkIndexer.index(source.read(indexArgs.input.apply()))
  }

}

