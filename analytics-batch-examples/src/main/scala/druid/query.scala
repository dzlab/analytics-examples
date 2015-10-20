package druid

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


case class TimeseriesQuery( 
  val dataSource: String, 
  val granularity: String, 
  val filter: Option[Filter], 
  val aggregations: List[Aggregation], 
  val postAggregations: List[PostAggregation], 
  val intervals: List[String]) {

 def toJson = JObject(
   "queryType" -> "timeseries",
   "dataSource" -> dataSource,
   "granularity" -> granularity,
   "filter" -> Utils.jsonfy(filter),
   "aggregations" -> aggregations.map(_.toJson),
   "postAggregation" -> postAggregations.map(_.toJson),
   "intervals" -> intervals
   ) 
}

case class TopNQuery (
  val dataSource: String,
  val dimension: String,
  val threshold: Int,
  val metric: String,
  val granularity: String,
  val filter: Option[Filter],
  val aggregas: List[Aggregation],
  val postAggregas: List[PostAggregation],
  val intervals: List[String]
  ) {
  def toJson() = {
    var fields: List[(String, JValue)] = List(
    "queryType" -> "topN",
    "dataSource" -> dataSource,
    "dimension" -> dimension,
    "threshold" -> threshold,
    "metric" -> metric,
    "granularity" -> granularity,
    "aggregations" -> aggregas.map(_.toJson),
    "postAggregations" -> postAggregas.map(_.toJson),
    "intervals" -> intervals
    )
    filter match {
      case Some(f) => fields = fields :+ ("filter", f.toJson)
      case None => fields  
    }
    JObject(fields)
  }
}

case class GroupByQuery(
  val dataSource: String,
  val granularity: String,
  val dimensions: List[String],
  val limitSpec: Option[LimitSpec],
  val filter: Option[Filter],
  val aggregas: List[Aggregation],
  val postAggregas: List[PostAggregation],
  val intervals: List[String],
  val having: Option[Having]
  ) {
  def toJson() = {
    var fields: List[(String, JValue)] = List(
      "queryType" -> "groupBy",
      "dataSource" -> dataSource,
      "granularity" -> granularity,
      "dimensions" -> dimensions,
      "aggregations" -> aggregas.map(_.toJson),
      "postAggregations" -> postAggregas.map(_.toJson),
      "intervals" -> intervals
    ) 
    limitSpec match {
      case Some(l) => fields = fields :+ ("limitSpec", l.toJson)
      case None => fields  
    } 
    filter match {
      case Some(f) => fields = fields :+ ("filter", f.toJson)
      case None => fields  
    } 
    having match {
      case Some(h) => fields = fields :+ ("having", h.toJson)
      case None => fields  
    }
    JObject(fields)
  }
}

case class Having(val typeName: String, val aggregation: String, val value: Int) extends JSONable {
  def toJson = JObject(
    "type" -> typeName,
    "aggregation" -> aggregation,
    "value" -> value
    )
}

case class LimitSpec(val typeName: String, val limit: Int, val columns: List[String]) extends JSONable {
  def toJson = JObject(
    "type" -> typeName,
    "limit" -> limit,
    "columns" -> columns
    )
}

