import java.io.File
import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import akka.stream.scaladsl.{Compression, FileIO, Flow, Framing, GraphDSL, Merge, Broadcast,Partition, RunnableGraph, Sink}
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.util.ByteString
import spray.json._
import TweetJsonProtocol._
import TweetType.TweetType
import SentimentAnalyzer.mainSentiment
import scala.util.{Failure, Success}

object TweetType extends Enumeration {
  type TweetType = Value
  val Normal, Retweet, Quoted, Reply = Value
}

object TweetStatistics extends App {
  import system.dispatcher

  implicit val system = ActorSystem("TwitterAnalysis")
  implicit val materializer = ActorMaterializer()
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

  val normalFlow = Flow[Tweet].scan(zero = 1)((acc,_)=> acc +1).map(acc => (TweetType.Normal,acc))
  val retweetFlow = Flow[Tweet].scan(zero = 1)((acc,_)=> acc +1).map(acc => (TweetType.Retweet,acc))
  val quotedFlow = Flow[Tweet].scan(zero = 1)((acc,_)=> acc +1).map(acc => (TweetType.Quoted,acc))
  val replyFlow = Flow[Tweet].scan(zero = 1)((acc,_)=> acc +1).map(acc => (TweetType.Reply,acc))

  val tweetUnmarshaller = Flow[ByteString].via(gunzip).buffer(50, OverflowStrategy.backpressure)
    .via(framingDecoder)
    .via(utf8DecoderFlow).via(filterEmpty).via(jsonParser).via(converterToTweets)

  val statistics = Flow[(TweetType,Int)]
    .grouped(20)
    .map(statisticsCreator)

  val sentimentFlow = Flow[Tweet]
    .map(tweet => {
      val text = tweet.full_text
      val ms = text //mainSentiment(text)
      s"Sentiment $ms | ${tweet.full_text} \n "
    })

  val fileSink = Flow[String]
    .map(i => ByteString(i))
    //.throttle(1, 1000.millis)
    .toMat(FileIO.toPath(
      new File("src/main/resources/sentiment.txt").toPath))((_, bytesWritten) => bytesWritten)

  val consoleSink = Sink.foreach(println)

val tweetsGraph = GraphDSL.create(fileSink, consoleSink)((fsink, _) => fsink) { implicit builder =>
  (filer, consoler) =>
    import GraphDSL.Implicits._
    val splitTweets = builder.add(Partition[Tweet](4, getTweetId))
    val mergeTweets = builder.add(Merge[(TweetType,Int)](4))
    val bcastTweet = builder.add(Broadcast[Tweet](2))

    val in = sourceTweetsCompressed
    in ~> tweetUnmarshaller.async ~> filterEN ~> splitTweets

    splitTweets.out(0) ~> bcastTweet
    bcastTweet.out(0) ~> sentimentFlow.async ~>filer
    bcastTweet.out(1) ~> normalFlow ~> mergeTweets.in(0)
    splitTweets.out(1) ~> retweetFlow ~> mergeTweets.in(1)
    splitTweets.out(2) ~> quotedFlow ~> mergeTweets.in(2)
    splitTweets.out(3) ~> replyFlow ~> mergeTweets.in(3)

    mergeTweets~> statistics ~> consoler

    ClosedShape
}

  val materialized = RunnableGraph.fromGraph(tweetsGraph).run()

  // shutdown
  materialized.onComplete {
    case Success(_) =>
      system.terminate()
    case Failure(e) =>
      println(s"Failure: ${e.getMessage}")
      system.terminate()
  }

  var normal, retweet, quoted, reply, total = 0
  def statisticsCreator(group: Seq[(TweetType,Int)]):String = {
    group.foreach(tupple =>{
    tupple._1 match {
      case TweetType.Normal => normal = tupple._2
      case TweetType.Retweet => retweet = tupple._2
      case TweetType.Quoted => quoted = tupple._2
      case TweetType.Reply => reply = tupple._2
    }
      total = normal + retweet + quoted + reply
    }
    )
    s"$total Tweets received. Count & percentages by type : "+ "\n" +
      f"~~> Normal Tweets : $normal ( ${(normal:Float)/total * 100}%.0f%%)" + "\n" +
      f"~~> Retweets      : $retweet ( ${(retweet:Float)/total * 100}%.0f%%)" + "\n" +
      f"~~> Quoted Tweets : $quoted ( ${(quoted:Float)/total * 100}%.0f%%)" + "\n" +
      f"~~> Reply Tweets  : $reply ( ${(reply:Float)/total * 100}%.0f%%)" + "\n"
  }


  def tweetIdentifier(tweet: Tweet):TweetType = {
    //println(tweet)
    if (tweet.retweeted_status.isDefined) TweetType.Retweet
    else if (tweet.quoted_status.isDefined) TweetType.Quoted
    else if (tweet.in_reply_to_status_id_str.isDefined) TweetType.Reply
    else TweetType.Normal
  }
  def getTweetId(tweet:Tweet):Int={
    tweetIdentifier(tweet).id
  }

}
