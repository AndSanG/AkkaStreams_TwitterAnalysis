import java.nio.file.Paths

import spray.json._
import TweetJsonProtocol._
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Compression, FileIO, Framing, Sink, Source}
import akka.http.scaladsl.common.JsonEntityStreamingSupport
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.stream.OverflowStrategy
import akka.util.ByteString

object Spray extends App {

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  implicit val system: ActorSystem = ActorSystem("TwitterAnalysis")

  val path = Paths.get("src/main/resources/assign1.json2.gz")
  val pathTest0 = Paths.get("src/main/resources/assign1.json2")
  val pathTest = Paths.get("src/main/resources/test2.json2")

  val twetsTest = FileIO.fromPath(path).via(Compression.gunzip())
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 20000, allowTruncation = true))
    .buffer(50, OverflowStrategy.backpressure)
    .map(_.utf8String)
    .filter(_!="{}")
    .map(source => source.parseJson)
    .map(jsonAst => jsonAst.convertTo[Tweet])
    .filter(tweet => tweet.lang == "en")
    .to(Sink.foreach(f => println(s"Tweet : '${f}'")))

  //twetsTest.run()
}


