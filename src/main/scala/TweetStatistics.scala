import java.nio.file.Paths

import Spray.jsonStreamingSupport
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape}
import akka.stream.scaladsl.{Balance, Compression, FileIO, Flow, Framing, GraphDSL, Merge, Partition, Sink}
import akka.util.ByteString
import spray.json._
import TweetJsonProtocol._
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}


object TweetStatistics extends App {
  implicit val actorSystem = ActorSystem("TwitterAnalysis")
  implicit val actorMaterializer = ActorMaterializer()
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  val path = Paths.get("src/main/resources/assign1.json2.gz")
  val pathTest0 = Paths.get("src/main/resources/assign1.json2")
  val pathTest = Paths.get("src/main/resources/test2.json2")

  //Sources
  val sourceTweetsCompressed = FileIO.fromPath(path)
  val sourceTest = FileIO.fromPath(pathTest0)

  //Flows

  val gunzip : Flow[ByteString,ByteString,NotUsed] = Flow[ByteString].via(Compression.gunzip())
  val framingDecoder : Flow[ByteString,ByteString,NotUsed] = Flow[ByteString].via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 20000, allowTruncation = true))
  val utf8DecoderFlow : Flow[ByteString,String,NotUsed] = Flow[ByteString].map(frame => frame.utf8String)
  val jsonParser : Flow[String,JsValue,NotUsed]= Flow[String].map(source => source.parseJson)
  val converterToTweets: Flow[JsValue,Tweet,NotUsed] = Flow[JsValue].map(jsonAst => jsonAst.convertTo[Tweet])
  val filterEN : Flow[Tweet,Tweet, NotUsed] = Flow[Tweet].filter(tweet => tweet.lang == "en")
  val filterEmpty : Flow[String,String,NotUsed] = Flow[String].filter(_!="{}")

  //Sinks
  val printlnSink = Sink.foreach[Tweet](f => println(s"Tweet : '${f}'"))

  val tweetUnmarshaller = sourceTweetsCompressed.via(gunzip)
    .via(framingDecoder)
    .via(utf8DecoderFlow)
    .via(filterEmpty)
    .via(jsonParser)
    .via(converterToTweets)
    .via(filterEN)
    .to(printlnSink)

  tweetUnmarshaller.run()
  /*
  val consoleSink = Sink.foreach[Int](println)
  val normal : Flow[Tweet,Int,NotUsed] = Flow[Tweet].map(tweet => 1)

  val graph = GraphDSL.create(){implicit builder =>
    val partitioner = builder.add(Partition[Tweet](4,classifier))
    partitioner.out() ~>
      partitioner.out() ~>
      partitioner.out() ~>
      partitioner.out() ~>
  }


  val graph = GraphDSL.create(fileSink, consoleSink)((fsink, _) => fsink) { implicit builder =>
    (filer, consoler) =>
      import GraphDSL.Implicits._

      val partitioner = builder.add(Partition[Int](2, sorter))

      numberSource ~> partitioner
      partitioner.out(0) ~> filer
      partitioner.out(1) ~> consoler

      ClosedShape
  }

  val pancakeChef: Flow[ScoopOfBatter, Pancake, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder ⇒
      val dispatchBatter = builder.add(Balance[ScoopOfBatter](2))
      val mergePancakes = builder.add(Merge[Pancake](2))

      // Using two pipelines, having two frying pans each, in total using
      // four frying pans
      dispatchBatter
        .out(0) ~> fryingPan1.async ~> fryingPan2.async ~> mergePancakes.in(0)
      dispatchBatter
        .out(1) ~> fryingPan1.async ~> fryingPan2.async ~> mergePancakes.in(1)

      FlowShape(dispatchBatter.in, mergePancakes.out)
    })

  batter.via(pancakeChef).to(Sink.foreach[Pancake](println)).run()
  */



}
