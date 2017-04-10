package index

import client.HNItem
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * Created by arjunpuri on 3/26/17.
  */

/** API to index files into the ElasticSearch cluster **/
case class BulkIndexer(client: HNElasticCluster) {

  def init(): Unit = {
    client.createIndex(HNElasticCluster.ITEMS_INDEX)
  }

  def index(itemIterator: Iterator[HNItem]): Unit = {
    client.upsert(itemIterator)

  }

}

