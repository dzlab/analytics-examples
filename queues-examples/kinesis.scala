package aws.kinesis

import com.amazonaws.auth._
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

class Client(val credentials: AWSCredentials, val endpoint: String, val regionId: String, val serviceName: String = "kinesis") {
  val client: AmazonKinesisClient = {
      val client = new AmazonKinesisClient(credentials) //DefaultHomePropertiesFile)
      client.setEndpoint(endpoint, serviceName, regionId)
      client
    }
 
  def createStream(name: String, shards: Int) {
    val createStreamReq: CreateStreamRequest = new CreateStreamRequest();
    createStreamReq.setStreamName( name );
    createStreamReq.setShardCount( shards );
    client.createStream(createStreamReq)
  }
  
  def deleteStream(name: String) {
    val deleteStreamReq: DeleteStreamRequest = new DeleteStreamRequest()
    deleteStreamReq.setStreamName( name )
    client.deleteStream(deleteStreamReq)
  }

  def putRecords(name: String, records: java.util.List[PutRecordsRequestEntry]): PutRecordsResult = {
    require(records.size <= 500, "Records size can't exceed the 500 limit, see http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html")
    val putReq: PutRecordsRequest = new PutRecordsRequest() 
    putReq.setStreamName( name )
    putReq.setRecords(records)
    client.putRecords(putReq)
  }
}

object Client {
  def apply(provider: AWSCredentialsProvider, endpoint: String, regionId: String, serviceName: String): Client = {
    new Client(provider.getCredentials(), endpoint, regionId, serviceName)
  }
}
