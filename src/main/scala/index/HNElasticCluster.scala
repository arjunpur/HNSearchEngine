package index

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * Created by arjunpuri on 3/26/17.
  */
object HNElasticCluster {

  val CLUSTER_NAME = "hnsearch"

  def getClient(): PreBuiltTransportClient = {
    val settings = Settings.builder()
    settings.put("cluster.name", HNElasticCluster.CLUSTER_NAME)
    new PreBuiltTransportClient(settings.build())
  }

}
