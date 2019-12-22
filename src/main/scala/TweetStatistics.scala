import java.nio.file.Paths

import Spray.jsonStreamingSupport
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Compression, FileIO, Flow, Sink}
import akka.util.ByteString
import spray.json._
import TweetJsonProtocol._


object TweetStatistics extends App {
  implicit val actorSystem = ActorSystem("TwitterAnalysis")
  implicit val actorMaterializer = ActorMaterializer()

  val pathTest = Paths.get("src/main/resources/test.json2")
  val path = Paths.get("src/main/resources/assign1.json2.gz")

  //Sources
  val sourceTest = FileIO.fromPath(pathTest)
  val sourceTweetsCompressed = FileIO.fromPath(path)

  //Flows
  val gunzip : Flow[ByteString,ByteString,NotUsed] = Flow[ByteString].via(Compression.gunzip())
  val utf8LowercaseFlow : Flow[ByteString,String,NotUsed] = Flow[ByteString].map(_.utf8String.toLowerCase)
  val splitter : Flow[ByteString,ByteString,NotUsed] = Flow[ByteString].via(jsonStreamingSupport.framingDecoder)
  val jsonParser : Flow[String,JsValue,NotUsed]= Flow[String].map(source => source.parseJson)
  val converterToTweets: Flow[JsValue,Tweet,NotUsed] = Flow[JsValue].map(jsonAst => jsonAst.convertTo[Tweet])


}
