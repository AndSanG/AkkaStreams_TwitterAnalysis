
import spray.json.{DefaultJsonProtocol, JsonFormat}
import spray.json._

case class Retweet(created_at : String, id_str : String, full_text : String,lang : String)

object RetweetJsonProtocol extends DefaultJsonProtocol{
  implicit object RetweetFormat extends RootJsonFormat[Retweet]{
    def write(obj: Retweet): JsValue = ???
    def read(json: JsValue): Retweet = {
      val fields = json.asJsObject.fields
      Retweet(
        fields("created_at").convertTo[String],
        fields("id_str").convertTo[String],
        fields("full_text").convertTo[String].replaceAll(System.lineSeparator(),""),
        fields("lang").convertTo[String]
      )
    }
  }
}

case class Quote(created_at : String, id_str : String, full_text : String,lang : String)

object QuoteJsonProtocol extends DefaultJsonProtocol{
  implicit object QuoteFormat extends RootJsonFormat[Quote]{
    def write(obj: Quote): JsValue = ???
    def read(json: JsValue): Quote = {
      val fields = json.asJsObject.fields
      Quote(
        fields("created_at").convertTo[String],
        fields("id_str").convertTo[String],
        fields("full_text").convertTo[String].replaceAll(System.lineSeparator(),""),
        fields("lang").convertTo[String]
      )
    }
  }
}

case class Tweet(created_at : String, id_str : String, full_text : String, lang : String, retweeted_status: Option[Retweet],quoted_status: Option[Quote],in_reply_to_status_id_str: Option[String])

object TweetJsonProtocol extends DefaultJsonProtocol{
  import RetweetJsonProtocol._
  import QuoteJsonProtocol._
  implicit object TweetFormat extends RootJsonFormat[Tweet]{
    def write(obj: Tweet): JsValue = ???

    def read(json: JsValue): Tweet = {
      val fields = json.asJsObject.fields

      Tweet(
        fields("created_at").convertTo[String],
        fields("id_str").convertTo[String],
        fields("full_text").convertTo[String].replaceAll(System.lineSeparator(),""),
        fields("lang").convertTo[String],
        fields.get("retweeted_status").map(_.convertTo[Retweet]),
        fields.get("quoted_status").map(_.convertTo[Quote]),
        convert(fields("in_reply_to_status_id_str"))
      )
    }
    def convert(value: JsValue) : Option[String]={
      value.toString()
      match{
        case "null" => None
        case _ => Option(value.convertTo[String])
      }
    }
  }
}

object test extends App {

  import TweetJsonProtocol._
  val json =
    """
      |{
      |  "created_at": "Mon Nov 04 14:37:20 +0000 2019",
      |  "id": 1191363805185478656,
      |  "id_str": "1191363805185478656",
      |  "full_text": "RT @kedehcodes: THis is what i get to do each day as a front end web developers",
      |  "in_reply_to_status_id_str": "1191363805185478656",
      |  "lang": "en",
      |  "quoted_status": {
      |      "created_at": "Mon Nov 04 09:40:06 +0000 2019",
      |      "id": 1191289006027419648,
      |      "id_str": "1191289006027419648",
      |      "full_text": "Cash &amp; carry strategies can return up to 10% annualised\n\n\u27a1\ufe0f Live curve on https://t.co/0KkntYiyQp\n\nDisclaimer: Not investment advice https://t.co/yqX9LK8XJm",
      |      "lang": "en"
      |   }
      |}
      |""".stripMargin

  println(json.parseJson.convertTo[Tweet])

}


