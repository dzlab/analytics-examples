package couchbase

import common._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject

object CouchbaseUtils {
  def toJson(values: Seq[String]): JsonDocument = {
    val obj: JsonObject = JsonObject.empty()
    (anx.COLUMNS zip values).foreach{case(k, v) => obj.put(k, if(v == "NULL") 0 else v)}
    obj.put("type", "auction")
    val doc: JsonDocument = JsonDocument.create(obj.get("auction_id_64").asInstanceOf[String], obj)
    doc 
  }
}
