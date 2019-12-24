import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy}
import akka.stream.scaladsl.{Compression, FileIO, Flow, Framing, GraphDSL, Merge, Partition, Sink}
import akka.util.ByteString
import spray.json._
import TweetJsonProtocol._
import TweetType.TweetType
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}

object TweetType extends Enumeration {
  type TweetType = Value
  val Normal, Retweet, Quote, Reply = Value
}

object TweetStatistics extends App {

  implicit val actorSystem = ActorSystem("TwitterAnalysis")
  implicit val actorMaterializer = ActorMaterializer()
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  val path = Paths.get("src/main/resources/assign1.json2.gz")
  val pathTest0 = Paths.get("src/main/resources/assign1.json2")
  val pathTest = Paths.get("src/main/resources/test2.json2")

  //Source
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
  val typeFlow : Flow[Tweet,TweetType,NotUsed] = Flow[Tweet].map(tweet => tweetIdentifier(tweet))
  //Sinks
  val printlnSink = Sink.foreach[TweetType](f => println(s"Tweet : '${f}'"))
  val consoleSink = Sink.foreach[TweetType](println)

  val tweetUnmarshaller = sourceTweetsCompressed.via(gunzip).buffer(50, OverflowStrategy.backpressure)
    .via(framingDecoder)
    .via(utf8DecoderFlow).via(filterEmpty).via(jsonParser).via(converterToTweets)
    .via(filterEN)

  val tweetCategories: Flow[Tweet,TweetType,NotUsed] =
    Flow.fromGraph(GraphDSL.create(){ implicit builder ⇒
      import GraphDSL.Implicits._
      val splitTweets = builder.add(Partition[Tweet](4, getTweetId))
      val mergeTweets = builder.add(Merge[TweetType](4))

      splitTweets.out(0) ~> typeFlow ~> mergeTweets.in(0)
      splitTweets.out(1) ~> typeFlow ~> mergeTweets.in(1)
      splitTweets.out(2) ~> typeFlow ~> mergeTweets.in(2)
      splitTweets.out(3) ~> typeFlow ~> mergeTweets.in(3)

      FlowShape(splitTweets.in,mergeTweets.out)
    })

  tweetUnmarshaller.via(tweetCategories)
    .grouped(20)
    .map(statisticsCreator)
    .to(Sink.foreach(println)).run()

  def statisticsCreator(group: Seq[TweetType]):String = {
    val normal = group.collect{case TweetType.Normal => }.length
    val retweet =group.collect{case TweetType.Retweet =>}.length
    val quoted =group.collect{case TweetType.Quote =>}.length
    val reply =group.collect{case TweetType.Reply =>}.length
    val total = normal + retweet + quoted + reply

    s"$total Tweets received. Count & percentages by type : "+ "\n" +
      f"-> Normal Tweets : $normal ( ${(normal:Float)/total * 100}%.0f%%)" + "\n" +
      f"-> Retweets      : $retweet ( ${(retweet:Float)/total * 100}%.0f%%)" + "\n" +
      f"-> Quoted Tweets : $quoted ( ${(quoted:Float)/total * 100}%.0f%%)" + "\n" +
      f"-> Reply Tweets  : $reply ( ${(reply:Float)/total * 100}%.0f%%)" + "\n"
  }


  def tweetIdentifier(tweet: Tweet):TweetType = {
    //println(tweet)
    if (tweet.retweeted_status.isDefined) TweetType.Retweet
    else if (tweet.quoted_status.isDefined) TweetType.Quote
    else if (tweet.in_reply_to_status_id_str.isDefined) TweetType.Reply
    else TweetType.Normal
  }
  def getTweetId(tweet:Tweet):Int={
    tweetIdentifier(tweet).id
  }

  //from https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html
  def reduceByKey[In, K, Out](maximumGroupSize: Int, groupKey: (In) => K, map: (In) => Out)(
    reduce: (Out, Out) => Out): Flow[In, (K, Out), NotUsed] = {
    Flow[In]
      .groupBy[K](maximumGroupSize, groupKey)
      .map(e => groupKey(e) -> map(e))
      .reduce((l, r) => l._1 -> reduce(l._2, r._2))
      .mergeSubstreams
  }
}
