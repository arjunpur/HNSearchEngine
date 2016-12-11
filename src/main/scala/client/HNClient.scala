package client

import java.net.URL

import akka.actor.ActorSystem
import akka.event.Logging
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import spray.http.{HttpResponse, HttpRequest}
import spray.httpx.SprayJsonSupport
import scala.concurrent.Future
import scala.concurrent.duration._
import spray.json.{JsonFormat, DefaultJsonProtocol}
import spray.client.pipelining._
import spray.util._
import scala.util.{Failure, Success}


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
case class HNPost(id: Long, deleted: Option[String], `type`: Option[String], by: Option[String],
                  time: Option[Long], text: Option[String], dead: Option[Boolean], parent: Option[Long],
                  kids: Option[List[Long]], url: Option[String], score: Option[Int], title: Option[String],
                  parts: Option[List[Long]], descendants: Option[Int])

object HNJsonProtocol extends DefaultJsonProtocol {
  implicit val post = jsonFormat14(HNPost.apply)
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
  val postPipeline: HttpRequest => Future[HNPost]  = sendReceive ~> unmarshal[HNPost]
  val maxIdPipeline: HttpRequest ⇒ Future[String] = sendReceive ~> unmarshal[String]

}

class HNClient {
  import HNClient._
  import system.dispatcher

  def item(id: Long): Future[HNPost] = {
    val url = ItemRequest(id).url
    execute(url, postPipeline)
  }

  def maxItemId(): Future[String] = {
    val url = MaxItemIdRequest.url
    execute(url, maxIdPipeline)
  }

  def execute[T](req: String, pipeline: HttpRequest => Future[T]) = {
    log.info(s"Executing: $req")
    val future = pipeline(Get(req))
    future.onComplete {
      case Success(res) =>
        log.info(s"Request: $req success")
      case Failure(res) =>
        log.error(s"Request: $req failed due to: $res")
        IO(Http).ask(Http.CloseAll)(1.second).await
    }
    future
  }


}



