package doradus 

import druid.{HttpUtils, Utils}
import common._
import com.twitter.util.FuturePool
import java.util.concurrent.Executors
import nl.grons.metrics.scala._

object Global {
  /** The application wide metrics registry. */
  val metricRegistry = new com.codahale.metrics.MetricRegistry()
}

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {
  val metricRegistry = Global.metricRegistry
}

object App extends Instrumented {

  val BATCH_SIZE = 10000

  val queryTimer: Timer = metrics.timer("aggregation") 
  val batchTimer: Timer = metrics.timer("batch")

  lazy val futurePool: FuturePool = {
    val processors = Runtime.getRuntime().availableProcessors() - 1
    val threadPool = Executors.newFixedThreadPool(processors)
    println(s"Creating a pool of $processors threads")
    FuturePool(threadPool)
  }

  def main(args: Array[String]) {
    insert()
    query()
  }

  def insert() {
    schema_upload()
    batch_upload("/Users/bachir/Workspace/DB/Doradus/standard_feed_withheader.csv")
  }

  def query() {
    queryTimer.time {
      HttpUtils.doGet("localhost", 1123, "anx/standard_feed/_aggregate?m=DISTINCT(event_type)")
    }
    queryTimer.time {
      HttpUtils.doGet("localhost", 1123, "anx/standard_feed/_aggregate?m=DISTINCT(is_click)")
    }

    println(s"Query timer stats: count=${queryTimer.count}, max=${queryTimer.max/1000000000.0}s, min=${queryTimer.min/1000000000.0}s, mean=${queryTimer.mean/1000000000.0}s")
  }

  def schema_upload() {
    // push application schema
    val std_feed_fields = List(
      ScalarField("auction_id_64", "Long", false),
      ScalarField("datetime", "Timestamp", false),
      ScalarField("user_tz_offset", "Integer", false),
      ScalarField("width", "Integer", false),
      ScalarField("height", "Integer", false),
      ScalarField("media_type", "Integer", false),
      ScalarField("fold_position", "Integer", false),
      ScalarField("event_type", "Text", false),
      ScalarField("imp_type", "Integer", false),
      ScalarField("payment_type", "Integer", false),
      ScalarField("media_cost_dollars_cpm", "Double", false),
      ScalarField("revenue_type", "Integer", false),
      ScalarField("buyer_spend", "Double", false),
      ScalarField("buyer_bid", "Double", false), // 'buyer-bid' is not a valid Doradus field name
      ScalarField("ecp", "Double", false),
      ScalarField("eap", "Double", false),
      ScalarField("is_imp", "Integer", false),
      ScalarField("is_learn", "Integer", false),
      ScalarField("predict_type_rev", "Integer", false),
      ScalarField("user_id_64", "Long", false),
      ScalarField("ip_address", "Text", false),
      ScalarField("ip_address_trunc", "Text", false),
      ScalarField("geo_country", "Text", false),
      ScalarField("geo_region", "Text", false),
      ScalarField("operating_system", "Integer", false),
      ScalarField("browser", "Integer", false),
      ScalarField("language", "Integer", false),
      ScalarField("value_id", "Integer", false),
      ScalarField("seller_member_id", "Integer", false),
      ScalarField("publisher_id", "Integer", false),
      ScalarField("site_id", "Integer", false),
      ScalarField("site_domain", "Text", false),
      ScalarField("tag_id", "Integer", false),
      ScalarField("external_inv_id", "Integer", false),
      ScalarField("reserve_price", "Double", false),
      ScalarField("seller_revenue_cpm", "Double", false),
      ScalarField("media_buy_rev_share_pct", "Double", false),
      ScalarField("pub_rule_id", "Integer", false),
      ScalarField("seller_currency", "Text", false),
      ScalarField("publisher_currency", "Text", false),
      ScalarField("publisher_exchange_rate", "Double", false),
      ScalarField("serving_fees_cpm", "Double", false),
      ScalarField("serving_fees_revshare", "Double", false),
      ScalarField("buyer_member_id", "Integer", false),
      ScalarField("advertiser_id", "Integer", false),
      ScalarField("brand_id", "Integer", false),
      ScalarField("advertiser_frequency", "Integer", false),
      ScalarField("advertiser_recency", "Integer", false),
      ScalarField("insertion_order_id", "Integer", false),
      ScalarField("campaign_group_id", "Integer", false),
      ScalarField("campaign_id", "Integer", false),
      ScalarField("creative_id", "Integer", false),
      ScalarField("creative_freq", "Integer", false),
      ScalarField("creative_rec", "Integer", false),
      ScalarField("cadence_modifier", "Double", false),
      ScalarField("can_convert", "Integer", false),
      ScalarField("user_group_id", "Integer", false),
      ScalarField("is_control", "Integer", false),
      ScalarField("controller_pct", "Double", false),
      ScalarField("controller_creative_pct", "Integer", false),
      ScalarField("is_click", "Integer", false),
      ScalarField("pixel_id", "Integer", false),
      ScalarField("is_remarketing", "Integer", false),
      ScalarField("post_click_conv", "Integer", false),
      ScalarField("post_view_conv", "Integer", false),
      ScalarField("post_click_revenue", "Double", false),
      ScalarField("post_view_revenue", "Double", false),
      ScalarField("order_id", "Text", false),
      ScalarField("external_data", "Text", false),
      ScalarField("pricing_type", "Text", false),
      ScalarField("booked_revenue_dollars", "Double", false),
      ScalarField("booked_revenue_adv_curr", "Double", false),
      ScalarField("commission_cpm", "Double", false),
      ScalarField("commission_revshare", "Double", false),
      ScalarField("auction_service_deduction", "Double", false),
      ScalarField("auction_service_fees", "Double", false),
      ScalarField("creative_overage_fees", "Double", false),
      ScalarField("clear_fees", "Double", false),
      ScalarField("buyer_currency", "Text", false),
      ScalarField("advertiser_currency", "Text", false),
      ScalarField("advertiser_exchange_rate", "Double", false),
      ScalarField("latitude", "Text", false),
      ScalarField("longitude", "Text", false),
      ScalarField("device_unique_id", "Text", false),
      ScalarField("device_id", "Integer", false),
      ScalarField("carrier_id", "Integer", false),
      ScalarField("deal_id", "Integer", false),
      ScalarField("view_result", "Integer", false),
      ScalarField("application_id", "Text", false),
      ScalarField("supply_type", "Integer", false),
      ScalarField("sdk_version", "Text", false),
      ScalarField("ozone_id", "Integer", false),
      ScalarField("billing_period_id", "Integer", false)
      )
    val std_feed_table = Table("standard_feed", std_feed_fields, List())
    val bid_landscape = Table("bid_landscape", List(), List())
    val storageOption =  DOption("StorageService", "OLAPService")
    val app = Application("anx", "anx_key", List(storageOption), List(std_feed_table, bid_landscape))

    HttpUtils.doDelete("localhost", 1123, "_applications/anx/anx_key")
    // ingest the application schema
    HttpUtils.doPost("localhost", 1123, "_applications", Utils.stringfy(app.toJson))
    Utils.write("anx.json", Utils.pretify(app.toJson))
  }

  def batch_upload(filename: String) {
    // push batch
    val reader = CSVUtils.read(filename, false)
    var docs: List[Doc] = List()
    reader.readNext match {
      case Some(headers) => {
        reader.iterator.foreach(values => {
          val doc = Doc("standard_feed", (headers zip values))
          docs = docs :+ doc
          if (docs.size == BATCH_SIZE) {
            submit(Batch(docs))
            docs = List()
          }
        })
        println(s"Batch timer stats: count=${batchTimer.count}, max=${batchTimer.max/1000000000.0}s, min=${batchTimer.min/1000000000.0}s, mean=${batchTimer.mean/1000000000.0}s")
      }
      case None =>
    }
/*
    reader.iteratorWithHeaders.foreach(dic  => {
      val doc = Doc("standard_feed", dic.toList)
      docs = docs :+ doc
      if (docs.size == BATCH_SIZE) {
        submit(Batch(docs))
        docs = List()
      }
    })
    */
    reader.close
    if(docs.size > 0)
      submit(Batch(docs))
  }

  def submit(batch: Batch) {
    //futurePool {
      batchTimer.time {
        HttpUtils.doPost("localhost", 1123, "anx/standard_feed", Utils.stringfy(batch.toJson))
      }
      //println (result.contentString)
    //}
  } 
}

