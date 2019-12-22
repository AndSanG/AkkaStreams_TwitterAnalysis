import spray.json.{DefaultJsonProtocol, JsonFormat}
import spray.json._


object NestedObject extends App with DefaultJsonProtocol{
  case class Meta(offset: Int, limit: Int, count: Int, total: Int)

  case class User(id: String, name: String, age: Int)

  case class Response(status: String, meta: Meta, users: List[User])

  /**
   * Handle nested object
   */
  def handleResponse(): Unit = {
    implicit val metaFormat: JsonFormat[Meta] = jsonFormat4(Meta)
    implicit val userFormat: JsonFormat[User] = jsonFormat3(User)
    implicit val responseFormat: JsonFormat[Response] = jsonFormat3(Response)
    val obj = Response("Ok",
      Meta(0, 10, 3, 243),
      List(
        User("001", "lambda", 26),
        User("002", "ops", 28),
        User("003", "rahasak", 26)
      )
    )
    println(obj.toJson.toString())

    val json =
      """
        |{
        |  "status":"ok"
        |  "meta":{"count":3,"limit":10,"offset":0,"total":243},
        |  "users":[
        |    {"age":26,"id":"001","name":"lambda"},
        |    {"age":28,"id":"002","name":"ops"},
        |    {"age":26,"id":"003","name":"rahasak"}
        |  ]
        |}
    """.stripMargin
    println(json.parseJson.convertTo[Response])

  }

  handleResponse()
  /*
   * output
  {"meta":{"count":3,"limit":10,"offset":0,"total":243},"users":[{"age":26,"id":"001","name":"lambda"},{"age":28,"id":"002","name":"ops"},{"age":26,"id":"003","name":"rahasak"}]}
  Response(Meta(0,10,3,243),List(User(001,lambda,26), User(002,ops,28), User(003,rahasak,26)))
  */
}

object CustomRead extends App with DefaultJsonProtocol{

  case class Status(code: String, desc: String)

  object StatusProtocol extends DefaultJsonProtocol {

    implicit object StatusFormat extends RootJsonFormat[Status] {
      def write(obj: Status) = ???

      def read(json: JsValue): Status =
        json.asJsObject.getFields("OUTRESPONSEDATA") match {
          case Seq(obj: JsObject) =>
            obj.getFields("OUTSTATUS", "OUTRESULTDESC") match {
              case Seq(JsString(c), JsString(d)) =>
                Status(c, d)
              case unrecognized => deserializationError(s"json serialization error $unrecognized")
            }
          case unrecognized => deserializationError(s"json serialization error $unrecognized")
        }
    }

  }

  /**
   * Custom read
   */
  def handleStatus(): Unit = {
    import StatusProtocol._
    val json =
      """
      {
        "OUTRESPONSEDATA":{
          "OUTSTATUS":"1",
          "OUTRESULTDESC": "ACCOUNT-POSTING COMPLETED"
        }
      }
    """.stripMargin
    println(json.parseJson.convertTo[Status])
  }

  handleStatus()
  /*
   * output
  Status(1,ACCOUNT-POSTING COMPLETED)
  */

}

object NestedRead extends App with DefaultJsonProtocol{
  //package com.rahasak.connect.protocol

  import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}

  case class Pagination(offset: Int, count: Int, nextPage: Option[String])

  object PaginationProtocol extends DefaultJsonProtocol {

    implicit object HeaderFormat extends RootJsonFormat[Pagination] {
      def write(obj: Pagination) = ???

      def read(json: JsValue): Pagination = {
        val fields = json.asJsObject.fields
        Pagination(
          fields("offset").convertTo[Int],
          fields("count").convertTo[Int],
          fields("next_page").convertTo[Option[String]]
        )
      }
    }

  }

  case class SearchHit(id: String, name: Option[String], pageContent: Option[String])

  object SearchHitProtocol extends DefaultJsonProtocol {

    implicit object SearchHitFormat extends RootJsonFormat[SearchHit] {
      def write(obj: SearchHit) = ???

      def read(json: JsValue): SearchHit = {
        val fields = json.asJsObject.fields
        SearchHit(
          fields("id").convertTo[String],
          fields("name").convertTo[Option[String]],
          fields.get("page_content").map(_.convertTo[String])
        )
      }
    }

  }

  case class Result(pagination: Pagination, searchHits: List[SearchHit])

  object ResultProtocol extends DefaultJsonProtocol {

    import PaginationProtocol._
    import SearchHitProtocol._

    implicit object ResultFormat extends RootJsonFormat[Result] {
      def write(obj: Result) = ???

      def read(json: JsValue): Result = {
        val fields = json.asJsObject.fields
        Result(
          fields("pagination").convertTo[Pagination],
          fields("search_hits").convertTo[List[SearchHit]]
        )
      }
    }

  }

  /**
   * Handle nested with custom read
   */
  def handleResult(): Unit = {
    import ResultProtocol._
    val json =
      """
        |{
        |  "pagination":{"count":3,"offset":0,"next_page":"https://lekana.com/api/v1/docs?offset=10"},
        |  "search_hits":[
        |    {"id":"001","name":"lambda","page_content":"lambda-ops"},
        |    {"id":"002","name":"ops"},
        |    {"id":"003","name":"rahasak", "page_content": "rahasak-labs"}
        |  ]
        |}
    """.stripMargin
    println(json.parseJson.convertTo[Result])
  }

  handleResult()
  /*
   * output
  Result(
    Pagination(0,3,Some(https://lekana.com/api/v1/docs?offset=10)),
    List(
      SearchHit(001,Some(lambda),Some(lambda-ops)),
      SearchHit(002,Some(ops),None),
      SearchHit(003,Some(rahasak),Some(rahasak-labs))
    )
  )
  */
}