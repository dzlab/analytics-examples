package cassandra

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.rogach.scallop._
import nl.grons.metrics.scala._
import common._

class AppConf(args: Array[String]) extends ScallopConf(args) {
  val auctions = opt[String](required = false, descr = "path to auctions (i.e. tandard_feed) file")
  val c = opt[Boolean](required = false, descr = "Create anx tables")
  val o = opt[String](required = false, descr = "Output directory where Cassandra tables will be stored")
}

object SparkSample extends Instrumented {

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")

  def create_schema(conf: SparkConf) {
    CassandraConnector(conf).withSessionDo { session =>
      // create database schema
      session.execute("DROP KEYSPACE IF EXISTS anx")
      session.execute("CREATE KEYSPACE anx WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS anx.auctions (auction_id_64 bigint PRIMARY KEY, datetime timestamp, user_tz_offset int, width int, height int, media_type int, fold_position int, event_type text, imp_type int, payment_type int, media_cost_dollars_cpm double, revenue_type int, buyer_spend double, buyer_bid double, ecp double, eap double, is_imp int, is_learn int, predict_type_rev int, user_id_64 bigint, ip_address text, ip_address_trunc text, geo_country text, geo_region text, operating_system int, brower int, language int, venue_id int, seller_member_id int, publisher_id int, site_id int, site_domain text, tag_id int, external_inv_id int, reserve_price double, seller_revenue_cpm double, media_buy_rev_share_pct double, pub_rule_id int, seller_currency text, publisher_currency text, publisher_exchange_rate double, serving_fees_cpm double, serving_fees_revshare double, buyer_member_id int, advertiser_id int, brand_id int, advertiser_frequency int,  advertiser_recency int, insertion_order_id int, campaign_group_id int, campaign_id int, creative_id int, creative_freq int, creative_rec int, cadence_modifier double, can_convert int, user_group_id int, is_control int, controller_pct double, controller_creative_pct int, is_click int, pixel_id int, is_remarketing int, post_click_conv int, post_view_conv int, post_click_revenue double, post_view_revenue double, order_id text, external_data text, pricing_type text, booked_revenue_dollars double, booked_revenue_adv_curr double, commission_cpm double, commision_revshare double, auction_service_deduction double, auction_service_fees double, creative_overage_fees double, clear_fees double, buyer_currency text, advertiser_currency text, advertiser_exchange_rate double, latitude text, longitude text, device_unique_id text, device_id int, carrier_id int, deal_id int, view_result text, application_id text, supply_type text, sdk_version text, ozone_id int, billing_period_id int)")
      // create aggregation functions
      /*session.execute("USE anx;")
      session.execute("CREATE FUNCTION sumFunc(current double, candidate double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'if(current == null) return candidate; return current + candidate;'")
      session.execute("CREATE AGGREGATE sum(double) SFUNC sumFunc STYPE double INITCOND null;")
      */
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("local[*]", "test", conf)
    val appConf = new AppConf(args)
    appConf.c.get.map(create => if(create==true) create_schema(conf))
    appConf.auctions.get.map(filename => ingest_auctions(sc, filename))
    //aggregations_auctions(sc, conf)
    query_auctions(sc)
  }

  def aggregations_auctions(sc: SparkContext, conf: SparkConf) {
    val s1 = System.nanoTime
    CassandraConnector(conf).withSessionDo { session =>
      val result: ResultSet = session.execute("SELECT buyer_member_id, brand_id, SUM(seller_revenue_cpm) FROM anx.auctions;")
      while(result.isExhausted == false) {
        println(result.one)
      }
    }
    val e1 = System.nanoTime
    println(s"CQL querying CASSANDRA in ${(e1-s1)/1000000000.0}")
  }

  def query_auctions(sc: SparkContext) {
    // write an equivalent CQL query
    qt.time { 
    val rdd = sc.cassandraTable("anx", "auctions").select("buyer_member_id", "brand_id", "seller_revenue_cpm")
    val kv = rdd.map(r =>  (r.getInt("buyer_member_id"), r.getInt("brand_id")) -> r.getDouble("seller_revenue_cpm")).reduceByKey(_ + _).collect() 
    }
    //kv.foreach(println)
    println(s"Querying from CASSANDRA in ${qt.mean}")
  }

  def query_builder(sc: SparkContext, dimensions: ColumnRef) {
    val start = System.nanoTime
    val rdd = sc.cassandraTable("anx", "auctions").select(dimensions)
    val end = System.nanoTime
    //kv.foreach(println)
    println(s"Querying from CASSANDRA in ${(end-start)/1000000000.0}")
  }

  def ingest_wordscount(conf: SparkConf, sc: SparkContext, appConf: AppConf) {
    appConf.auctions.get.map(filename => {
      CassandraConnector(conf).withSessionDo { session => 
        session.execute("DROP KEYSPACE test")
        session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS test.words (word text PRIMARY KEY, count int)")
      }
      val lineRDD = sc.textFile(filename).map(_.split(","))
      val keys = List("word", "count")
      val rowRDD = lineRDD.map{values => CassandraRow.fromMap((keys zip values) toMap)}
      rowRDD.saveToCassandra("test", "words")
    })
  }

  def ingest_auctions(sc: SparkContext, filename: String) {
    val keys = List("auction_id_64", "datetime", "user_tz_offset", "width", "height", "media_type", "fold_position", "event_type", "imp_type", "payment_type", "media_cost_dollars_cpm", "revenue_type", "buyer_spend", "buyer_bid", "ecp", "eap", "is_imp", "is_learn", "predict_type_rev", "user_id_64", "ip_address", "ip_address_trunc", "geo_country", "geo_region", "operating_system", "brower", "language", "venue_id", "seller_member_id", "publisher_id", "site_id", "site_domain", "tag_id", "external_inv_id", "reserve_price", "seller_revenue_cpm", "media_buy_rev_share_pct", "pub_rule_id", "seller_currency", "publisher_currency", "publisher_exchange_rate", "serving_fees_cpm", "serving_fees_revshare", "buyer_member_id", "advertiser_id", "brand_id", "advertiser_frequency",  "advertiser_recency", "insertion_order_id", "campaign_group_id", "campaign_id", "creative_id", "creative_freq", "creative_rec", "cadence_modifier", "can_convert", "user_group_id", "is_control", "controller_pct", "controller_creative_pct", "is_click", "pixel_id", "is_remarketing", "post_click_conv", "post_view_conv", "post_click_revenue", "post_view_revenue", "order_id", "external_data", "pricing_type", "booked_revenue_dollars", "booked_revenue_adv_curr", "commission_cpm", "commision_revshare", "auction_service_deduction", "auction_service_fees", "creative_overage_fees", "clear_fees", "buyer_currency", "advertiser_currency", "advertiser_exchange_rate", "latitude", "longitude", "device_unique_id", "device_id", "carrier_id", "deal_id", "view_result", "application_id", "supply_type", "sdk_version", "ozone_id", "billing_period_id")
    bt.time {
    val lineRDD = sc.textFile(filename).map(_.split("\t"))
    val rowRDD = lineRDD.map{values => CassandraRow.fromMap((keys zip values.map(v => if(v == "NULL") 0 else v)) toMap)}
    // save standard feeds to cassandra
    rowRDD.saveToCassandra("anx", "auctions")
    }
    println(s"Wrote to CASSANDRA in ${bt.mean}")
  }
  
  def sample() {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext("local[*]", "test", conf)
    
    val rdd = sc.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)

    // save some data into cassandra
    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
  }
}
