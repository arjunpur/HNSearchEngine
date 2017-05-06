package index

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.client.PreBuiltTransportClient
import java.net.InetAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.client.transport.TransportClient
import com.typesafe.scalalogging.StrictLogging
import spray.json._
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import client.{HNItem, HNJsonProtocol}
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse

/**
  * Created by arjunpuri on 3/26/17.
  */
object HNElasticCluster {

  val ITEMS_INDEX = "items"

  def apply(clusterName: String, hostPorts: Seq[(String, Int)]) = {
    val settings = Settings.builder()
    settings.put("cluster.name", clusterName)
    val client = new PreBuiltTransportClient(settings.build())
      .addTransportAddresses(hostPorts.map {
        case (host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port)
      }:_*)
    new HNElasticCluster(client)
  }

}

class HNElasticCluster(client: TransportClient) extends StrictLogging {

  import HNJsonProtocol._
  
  /* Client to interact with indices */
  private val indicesClient: IndicesAdminClient = client.admin().indices()

  def createIndex(name: String): Unit = {
    if (!indexExists(name)) indicesClient.prepareCreate(name).get()
  }

  def indexExists(name: String): Boolean = {
    val res: IndicesExistsResponse = indicesClient.exists(new IndicesExistsRequest(HNElasticCluster.ITEMS_INDEX)).actionGet
    res.isExists
  }

  def upsert(items: Iterator[HNItem]): Unit = {

    logger.info(s"Upserting ${items.size} items to cluster")

    /* Upserts a single item */
    def upsertItem(item: HNItem): Unit = {
      if (item.id.isEmpty || item.`type`.isEmpty) throw new IllegalArgumentException("Cannot upsert item with no id or type")
      val itemType = item.`type`.get
      val id = item.id.get.toString
      val json = item.toJson.toString
      /* Create requests to submit to cluster */
      val indexRequest = new IndexRequest(HNElasticCluster.ITEMS_INDEX, itemType, id).source(json)
      val updateRequest = new UpdateRequest(HNElasticCluster.ITEMS_INDEX, itemType, id).doc(json).upsert(indexRequest)
      val res = client.update(updateRequest).get()
    }
    
    items.foreach(upsertItem)

  }


}
