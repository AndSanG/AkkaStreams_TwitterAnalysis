import java.nio.file.Paths

import spray.json._
import TweetJsonProtocol._
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, Compression, FileIO, Flow, GraphDSL, Merge, Sink}
import akka.http.scaladsl.common.JsonEntityStreamingSupport
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.stream.FlowShape
import edu.stanford.nlp.graph.Graph
import javax.swing.text.FlowView.FlowStrategy


object Spray extends App {

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  implicit val system: ActorSystem = ActorSystem("TwitterAnalysis")

  val path = Paths.get("src/main/resources/assign1.json2")
  val pathTest0 = Paths.get("src/main/resources/assign1.json2")
  val pathTest = Paths.get("src/main/resources/test2.json2")

  val twetsTest = FileIO.fromPath(pathTest0)//.via(Compression.gunzip())
    .via(jsonStreamingSupport.framingDecoder)
    .map(_.utf8String)
    .map(source => source.parseJson)
    .map(jsonAst => jsonAst.convertTo[Tweet])
    .filter(tweet => tweet.lang == "en")
    .to(Sink.foreach(f => println(s"Tweet : '${f}'")))

  //twetsTest.run()

  val tweetClassifier = Flow[Tweet,Tweet,NotUsed] =
    Flow.fromGraph(GraphDSL.create(){ implicit builder ⇒
      val splitTweets = builder.add(Balance[Tweet](2))
      val mergeTweets = builder.add(Merge[Tweet](2))
      val broadTweets = builder.add(Broadcast[Tweet](1))

      FlowShape(splitTweets.in,mergeTweets.out)
    })



}


