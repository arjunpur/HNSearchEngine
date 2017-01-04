package client

import java.net.URL

import akka.actor.ActorSystem
import akka.event.Logging
import spray.http.HttpRequest
import spray.httpx.SprayJsonSupport
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import spray.json.DefaultJsonProtocol
import spray.client.pipelining._
/**
  * Created by arjunpuri on 11/27/16.
  */

/* Requests */
trait Request {
  def hostUrl = new URL("https://hacker-news.firebaseio.com/v0")
  def url: String
}
case class ItemRequest(id: Long) extends Request {
  def url: String = hostUrl + s"/item/$id.json"
}

case object MaxItemIdRequest extends Request {
  def url: String = hostUrl + s"/maxitem.json"
}

/* JSON Protocol */
case class HNItem(id: Long, deleted: Option[String] = None, `type`: Option[String] = None, by: Option[String] = None,
                  time: Option[Long] = None, text: Option[String] = None, dead: Option[Boolean] = None, parent: Option[Long] = None,
                  kids: Option[List[Long]] = None, url: Option[String] = None, score: Option[Int] = None, title: Option[String] = None,
                  parts: Option[List[Long]] = None, descendants: Option[Int] = None)


object HNJsonProtocol extends DefaultJsonProtocol {
  implicit val post = jsonFormat14(HNItem.apply)
}

object HNClient {

  /* Async client needs a system */
  implicit val system = ActorSystem("HNClient")
  /* Get the execution context */
  import system.dispatcher
  val log = Logging(system, getClass)

  import HNJsonProtocol._
  import SprayJsonSupport._
  /* Pipeline to deal with requests to HNPost */
  val postPipeline: HttpRequest => Future[HNItem]  = sendReceive ~> unmarshal[HNItem]
  val maxIdPipeline: HttpRequest ⇒ Future[String] = sendReceive ~> unmarshal[String]

}

class HNClient {
  import HNClient._

  def item(id: Long): Future[HNItem] = {
    val url = ItemRequest(id).url
    execute(url, postPipeline)
  }

  def maxItemId(): Future[String] = {
    val url = MaxItemIdRequest.url
    execute(url, maxIdPipeline)
  }

  def execute[T](req: String, pipeline: HttpRequest => Future[T]): Future[T] = pipeline(Get(req))

  def shutdown(): Unit = {
    Await.result(system.terminate(), Duration.Inf)
  }


}



