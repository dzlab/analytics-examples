package druid 

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.twitter.finagle.{Httpx, Service}
import com.twitter.finagle.httpx.{Fields, MediaType, Method, Request, RequestBuilder, Response}
import com.twitter.util.{Await, Future}
import com.twitter.io.Buf //ByteArray

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.{Try, Success, Failure}

trait JSONable {
  def toJson: JValue
}

object Utils {

  def write(filename: String, content: String): Try[Path] = {
    val tried = Try(Files.write(Paths.get(s"./$filename"), content.getBytes()))
    tried match {
      case Success(p) => println(s"Succeeded to write content to $p")
      case Failure(e) => println(s"Failure to write caused by $e")
    }
    tried
  }

  def echo(json: JValue): Unit = {
    println(pretty(render(json)))
  }
  
  def pretify(json: JValue): String = {
    pretty(render(json))
  }

  def stringfy(json: JValue): String = {
    compact(render(json))
  }

  def jsonfy(jsonable: Option[JSONable]) = jsonable match { 
    case Some(x) => x.toJson
    case None => JObject()
  }
}

object HttpUtils {

  def doGet(host: String, port: Int, path: String): Response = {
    val url = s"http://$host:$port/$path"
    val client: Service[Request, Response] = Httpx.newService(s"$host:$port")
    val request = RequestBuilder()
            .url(url) 
            .buildGet()
    val response: Future[Response] = client(request)
    response.onSuccess { resp: Response =>
        println(s"GET $url success: $resp") // res.contentString
    }
    response.onFailure { cause: Throwable =>
        println(s"GET $url failed : $cause")
    }
    Await.result(response)
  }

  def doDelete(host: String, port: Int, path: String): Response = {
    val url = s"http://$host:$port/$path"
    val client: Service[Request, Response] = Httpx.newService(s"$host:$port")
    val request = RequestBuilder()
            .url(url) 
            .buildDelete()
    val response: Future[Response] = client(request)
    response.onSuccess { resp: Response =>
        println(s"DELETE $url success: $resp\n${resp}") // res.contentString
    }
    response.onFailure { cause: Throwable =>
        println(s"DELETE $url failed : $cause")
    }
    Await.result(response)
  }

  /**
   * see https://www.paypal-engineering.com/tag/finagle/ and https://twitter.github.io/finagle/guide/index.html
   * */
  def doPost(host: String, port: Int, path: String, content: String): Response = {
    val url = s"http://$host:$port/$path"
    val client: Service[Request, Response] = Httpx.newService(s"$host:$port")
    val bytes = content.getBytes
    val request = RequestBuilder()
            .url(url) 
            .setHeader("Content-Type", MediaType.Json)
            .buildPost(Buf.ByteArray(bytes, 0, bytes.length))
    val response: Future[Response] = client(request)
    response.onSuccess { resp: Response =>
        println(s"POST $url success: $resp") // res.contentString
    }
    response.onFailure { cause: Throwable =>
        println(s"POST $url failed : $cause")
    }
    Await.result(response)
  }

  def main(args: Array[String]) {
    val granularity = GranularitySpec("uniform", "HOUR", "NONE", List("2015-08-01/2015-09-01"))
    Utils.echo(granularity.toJson)
    val metrics = List(Aggregation("longSum", "clicks", "clicks"), Aggregation("longSum", "impressions", "impressions"), Aggregation("doubleSum", "revenue_deal", "revenue_deal"))
    //println(compact(render(metrics.toJson)))
    val dimensions = List("seller_id", "site_id", "brand_id", "buyer_id", "device_type", "platform", "media_type", "format", "publisher_id", "geo_country")
    val parser = Parser("string", JsonParseSpec("json", TimestampSpec("date", "auto"), DimensionsSpec(dimensions, List(), List())))
    println(compact(render(parser.toJson)))
    val schema = DataSchema("revenue_impression", parser, metrics, granularity)
    println(compact(render(schema.toJson)))
   
    val io = IOConfig("index", "local", "/Users/bachir/Workspace/DB/data", "revenue_impressions_data.json")
    println(compact(render(io.toJson)))
    val tuning = TuningConfig("index", 0, 0)
    println(compact(render(tuning.toJson)))
    val task = IndexTask("index", schema, io, tuning)

    doPost("localhost", 8090, "druid/indexer/v1/task", Utils.stringfy(task.toJson))

  }
}

