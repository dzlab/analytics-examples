package adk.endpoint

import com.twitter.finagle.Http
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Service
import com.twitter.finagle.http.{HttpMuxer, Request, Response}
import com.twitter.util.Await
import com.twitter.util.Future
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.http._

object httpServer {
  def main(args: Array[String]) {
    val mux: HttpMuxer = new HttpMuxer()
	.withHandler("/stream", new StreamService())
	.withHandler("/data", new DataService())
    val server: ListeningServer = Http.serve(new InetSocketAddress(8000), mux);

    println("The server Alive and Kicking");
    Await.ready(server);
  }
}
/*
class CalcService extends Service[Request, Response] {
  def apply(req: HttpRequest): Future[HttpResponse] = {
    val decoder: QueryStringDecoder = new QueryStringDecoder(req.getUri());
    val reqContent = req.getContent()

    // Do your calculation here.
    val param = Integer.parseInt(decoder.getParameters().get("param").get(0));

    val result: String = Integer.toString(param);

    val content: StringBuilder = new StringBuilder();
    content.append("<html><body>");
    content.append(result + "<br/>");
    content.append("</body></html>");

    val response: HttpResponse = new DefaultHttpResponse(
      req.getProtocolVersion(),
      HttpResponseStatus.OK
    );

    response.setContent(copiedBuffer(content.toString(), UTF_8));

    return Future.value(response);
  }
}*/
