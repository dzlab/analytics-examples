package doradus

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


case class Batch(val docs: List[Doc]) {
  def toJson = JObject(
    "batch" -> JObject(
      "docs" -> JArray(docs.map(_.toJson))
      )
    )
}

case class Doc(val table: String, val fields: List[(String, String)]){
  def toJson = {
    var docs: List[(String, JValue)] = List(("_table", JString(table)))
    fields.foreach(f => docs = docs :+ (f._1.replace("-", "_"), convert(f._1, f._2)))
    JObject(
      "doc" -> JObject(docs) 
    )
  }

  val strings: List[String] = List("datetime", "event_type", "ip_address", "ip_address_trunc", "geo_country", "geo_region", "site_domain", "seller_currency", "publisher_currency", "order_id", "external_data", "pricing_type", "buyer_currency", "advertiser_currency", "latitude", "longitude", "device_unique_id", "application_id", "sdk_version")
  val numerics: List[String] = List("media_cost_dollars_cpm", "buyer_spend", "buyer-bid", "ecp", "eap", "reserve_price", "seller_revenue_cpm", "media_buy_rev_share_pct", "publisher_exchange_rate", "serving_fees_cpm", "serving_fees_revshare", "cadence_modifier", "controller_pct", "post_click_revenue", "post_view_revenue", "booked_revenue_dollars", "booked_revenue_adv_curr", "commission_cpm", "commission_revshare", "auction_service_deduction", "auction_service_fees", "creative_overage_fees", "clear_fees", "advertiser_exchange_rate")
  def convert(key: String, value: String): JValue = {
    try {
    if(value.equals("NULL"))
      JNull
    else if(strings.contains(key))
      JString(value)
    else if(numerics.contains(key))
      JDouble(value.toDouble)
    else
      JInt(value.toLong)
    }catch {
      case e: Throwable => println (s"Failed to convert $key,$value -> $e"); JNull
    }
  }
}

case class Application(val name: String, val key: String, val options: List[DOption], val tables: List[Table]) {
  def toJson = JObject(
    name -> JObject(
      "key" -> key,
      "options" -> JObject(options.map(_.toJson)),
      "tables" -> JObject(tables.map(t => t.name -> t.toJson))
      )
    )
} 

case class DOption(val key: String, val value: String) {
  def toJson = (key, JString(value))
  /*def toJson = JObject(
    "StorageService" -> JString(storageService)
    )*/
}

case class Table(val name: String, val fields: List[Field], val aliases: List[Alias]) {
  def toJson = JObject(
    "fields" -> JObject(fields.map(f => f.name -> f.toJson)),
    "aliases" -> JObject(aliases.map(a => a.name -> a.toJson))
    )
}

trait Field {
  def name: String
  def toJson: JValue
}

case class ScalarField(val name: String, val typeName: String, val isCollection: Boolean) extends Field {
  def toJson = JObject(
    "collection" -> JBool(isCollection),
    "type" -> JString(typeName)
    )
}

case class LinkField(val name: String, val typeName: String, val table: String, val inverse: String) {
  require(typeName.equals("LINK") || typeName.equals("XLINK"), "Links type name can be either LINK or XLINK")

  def toJson = JObject(
    "name" -> name,
    "table" -> table,
    "type" -> "LINK",
    "inverse" -> inverse
    )
}

case class GroupField(val name: String, val fields: List[Field]) {
  def toJson = JObject(
    "name" -> name, 
    "fields" -> fields.map(_.toJson)
    )
}

case class Alias(val name: String, val expression: String) {
  require(name.startsWith("$"), "An alias name must start with $")
  
  def toJson = JObject(
    "expression" -> JString(expression)
    )
}

