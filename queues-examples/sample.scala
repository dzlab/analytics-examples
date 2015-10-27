package adk.endpoint

/*
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._
import io.github.cloudify.scala.aws.auth.CredentialsProvider.DefaultHomePropertiesFile
*/
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import auth.CredentialsProvider.DefaultHomePropertiesFile

import scala.collection.JavaConverters._
//import com.twitter.util.{Future, FuturePool}

object sample {

  def main(args: Array[String]) {
    withAmazon(args)
  }
  
  def withAmazon(args: Array[String]) {
    implicit val client: AmazonKinesisClient = {
      val client = new AmazonKinesisClient(DefaultHomePropertiesFile)
      val endpoint = "kinesis.eu-west-1.amazonaws.com"
      val serviceName = "kinesis"
      val regionId = "eu-west-1"
      client.setEndpoint(endpoint, serviceName, regionId)
      client
    }

    // create stream
    //val createStreamReq: CreateStreamRequest = new CreateStreamRequest();
    //createStreamReq.setStreamName( "DataStream" );
    //createStreamReq.setShardCount( 50 );
    //client.createStream(createStreamReq)

    // sleep until stream created
    Thread.sleep(100000)
    val before = System.nanoTime
    // push data to stream
    (1 to 2000).foreach{ i =>
      val putReq: PutRecordsRequest = new PutRecordsRequest() 
      putReq.setStreamName("DataStream")
      val putReqData = (1 to 500).map { j => 
        val putEntry: PutRecordsRequestEntry = new PutRecordsRequestEntry();
        putEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
        putEntry.setPartitionKey(s"partitionKey-$i-$j");
        putEntry     
      }.asJava
      putReq.setRecords(putReqData)
      Future { val putRes: PutRecordsResult = client.putRecords(putReq) }
    }
    val after = System.nanoTime
    
    Thread.sleep(10000)
    println(s"Put Result in ${(after-before)/1e9}")
    //println(s"Put Result $putRes in ${(after-before)/1e9}")
  }
/*
  def withCloudify(args: Array[String]) {
    // Declare an implicit Kinesis `Client` that will be used to make API calls.
    implicit val kinesisClient = Client.fromCredentials(DefaultHomePropertiesFile)

    // First we create the stream.
    val createStream = for {
      s <- Kinesis.streams.create("myStream")
    } yield s

    val s = Await.result(createStream, 60.seconds)
    println("stream created")

    // Stream creation takes some time, we must wait the stream to become active
    // before using it.
    // In this example we're going to wait for up to 60 seconds for the stream
    // to become active.
    val waitActive = Await.result(s.waitActive.retrying(60), 60.seconds)
    println("stream active")

    // Now that the stream is active we can fetch the stream description.
    val description = Await.result(s.describe, 10.seconds)
    println(description.status)
    println(description.isActive)

    // Then we put some data in it.
    // The `put` method expects a ByteBuffer of data and a partition key.
    val putData = for {
      _ <- s.put(ByteBuffer.wrap("hello".getBytes), "k1")
      _ <- s.put(ByteBuffer.wrap("how".getBytes), "k1")
      _ <- s.put(ByteBuffer.wrap("are you?".getBytes), "k2")
    } yield ()
    Await.result(putData, 30.seconds)
    println("data stored")

    // Then we can attempt to fetch the data we just stored.
    // To fetch the data we must iterate through the shards associated to the
    // stream and get records from each shard iterator.
    val getRecords = for {
      shards <- s.shards.list
      iterators <- Future.sequence(shards.map {
        shard =>
          implicitExecute(shard.iterator)
      })
      records <- Future.sequence(iterators.map {
        iterator =>
          implicitExecute(iterator.nextRecords)
      })
    } yield records
    val records = Await.result(getRecords, 30.seconds)
    println("data retrieved")
    Thread.sleep(10000)
    // Then we delete the stream.
    val deleteStream = for {
      _ <- s.delete
    } yield ()
    Await.result(deleteStream, 30.seconds)
    println("stream deleted")
  }
*/
}
