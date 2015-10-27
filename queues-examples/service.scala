package adk.endpoint

import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._
import io.github.cloudify.scala.aws.auth.CredentialsProvider.DefaultHomePropertiesFile

import com.twitter.finagle.http.{Method, Request, Response, Status, ParamMap}
import com.twitter.finagle.Service
import com.twitter.io.Reader
import com.twitter.util.Future
import com.twitter.logging.{Logger}

import scala.concurrent.{Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import java.io.ByteArrayInputStream

class AbstractService extends Service[Request, Response] {
  def apply(req: Request): Future[Response] = {
    val params = req.params 
    val reqContent = req.contentString
    
    val (content, status) = req.method match {
      case Method.Get    => doGet(params) // get resource
      case Method.Post   => doPost(params) // create resource
      case Method.Delete => doDelete(params)  // delete resource
    }
    val reader = Reader.fromStream(new ByteArrayInputStream(content.getBytes("UTF-8")))
    val resp: Response = Response(req.version, status, reader)
    return Future.value(resp)
  }
  def doGet(params: ParamMap): (String, Status) = ???
  def doPost(params: ParamMap): (String, Status) = ???
  def doDelete(params: ParamMap): (String, Status) = ???
}

class KinesisService extends AbstractService {
  implicit val kinesisClient = Client.fromCredentials(DefaultHomePropertiesFile)
}

class StreamService extends KinesisService {
  override def doGet(params: ParamMap): (String, Status) = {
    val name = params.getOrElse("name", "")
    val description = Await.result(Kinesis.stream(name).describe, 10.seconds)
    val content = s"{'status': '${description.status}', 'isActive': '${description.isActive}'}"
    return (content, Status.Ok) 
  }

  override def doPost(params: ParamMap): (String, Status) = {
    val name = params.getOrElse("name", "")
    val createStream = Kinesis.streams.create(name)
    val s = Await.result(createStream, 60.seconds)
    return ("stream created", Status.Accepted)
 }
  
  override def doDelete(params: ParamMap): (String, Status) = {
    val name = params.getOrElse("name", "")
    val deleteStream = Kinesis.stream(name).delete
    Await.result(deleteStream, 30.seconds)
    return ("stream deleted", Status.Accepted)
  }
}

class DataService extends KinesisService {
  import scalax.io._  
  import Line.Terminators.NewLine
  import java.nio.ByteBuffer

  //implicit val kinesisClient = Client.fromCredentials(DefaultHomePropertiesFile)
  val log = Logger.get (getClass)

  override def doPost(params: ParamMap): (String, Status) = {
    val name = params.getOrElse("name", "")
    val file = params.getOrElse("file", "")
    log.debug(s"Received a request for consuming events from file $file and push to $name")

    val s = Kinesis.stream(name)
println(s"Received a request for consuming events from file $file and push to $name")
    val before = System.nanoTime
    val count = Resource.fromFile(file).lines(NewLine,false).map(line => {
      val values = line.split("\t")
      val key = values(0)
      val putData = s.put(ByteBuffer.wrap(values.tail.reduce(_+"\t"+_).getBytes), key)
      putData
    }).map(_ => 1).reduce(_+_) //.foreach(putData => {Await.result(putData, 30.seconds)})
    val after = System.nanoTime
    val content = s"Stored $count in ${(after-before)/1e9}"
    val status = Status.Ok
    return (content, status)
  }

  override def doGet(params: ParamMap): (String, Status) = {
    val name = params.getOrElse("name", "")
    // read the steam data
    val s = Kinesis.stream(name)
    val getRecords = for {
      shards <- s.shards.list
      iterators <- scala.concurrent.Future.sequence(shards.map {
        shard => implicitExecute(shard.iterator)
      })
      records <- scala.concurrent.Future.sequence(iterators.map {
        iterator => implicitExecute(iterator.nextRecords)
      })
    } yield records
    val records = Await.result(getRecords, 30.seconds)
    val count = records
    val content = s"Found ${count} records"
    val status = Status.Ok
    return (content, status)
  }
}

