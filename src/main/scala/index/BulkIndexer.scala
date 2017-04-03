package index

import client.HNItem
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * Created by arjunpuri on 3/26/17.
  */

/** API to index files into the ElasticSearch cluster **/
case class BulkIndexer(client: PreBuiltTransportClient) {

  def index(itemIterator: Iterator[HNItem]): Unit = {
    itemIterator.take(100).foreach(println)


  }

}

