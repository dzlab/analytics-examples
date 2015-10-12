package monetdb

import java.sql.{DriverManager, Connection, PreparedStatement, ResultSet, ResultSetMetaData, Timestamp}
import scala.util.{Failure, Success, Try}
import common._
import nl.grons.metrics.scala._

object JavaSDK extends Instrumented {

  val MAX_BATCH_SIZE = 100

  val qt: Timer = metrics.timer("query")
  val wt: Timer = metrics.timer("write")
  val bt: Timer = metrics.timer("batch")
  val at: Timer = metrics.timer("aggregas")

  def main(args: Array[String]) {
    val ops = new Options(args)
    val node = ops.n.get.getOrElse("localhost")
    val size = ops.s.get.getOrElse(MAX_BATCH_SIZE)
    val input = ops.i.get.getOrElse("")
    Class.forName("nl.cwi.monetdb.jdbc.MonetDriver") // load the driver
    val con: Connection = DriverManager.getConnection(s"jdbc:monetdb://$node/${anx.KEYSPACE}", "monetdb", "monetdb")
    Try {
      init()(con)
      bt.time { upload(input, size)(con) } 
      query()(con)
      } match {
        case Success(future) => println("success")
        case Failure(t) => t.printStackTrace
    }
    con.close()
    println(s"Uploading data to MonetDB in count: ${bt.count}, max: ${(bt.max)/1000000000.0}s, min:${bt.min/1000000000.0}s, mean:${bt.mean/1000000000.0}s")
    println(s"Querying auctions by id in count: ${qt.count}, max: ${(qt.max)/1000000.0}ms, min:${qt.min/1000000.0}ms, mean:${qt.mean/1000000.0}ms")
    println(s"Writing a MonetDB row in count: ${wt.count}, max: ${(wt.max)/1000000.0}ms, min:${wt.min/1000000.0}ms, mean:${wt.mean/1000000.0}ms")
    println(s"Aggregation on auctions in count: ${at.count}, max: ${(at.max)/1000000.0}ms, min:${at.min/1000000.0}ms, mean:${at.mean/1000000.0}ms")
  }

  def upload(input: String, size: Int)(implicit con: Connection) {
    val insert = anx.INSERT_STMT.replace(anx.KEYSPACE+"."+anx.TABLE_STD_FEED, anx.TABLE_STD_FEED) + ";" 
    //val batch = "START TRANSACTION;" + insert * size + "COMMIT;"
    val st: PreparedStatement = con.prepareStatement(insert) //batch)
    println(s"Executing batches of $size")

    val reader = new Reader(input)
    reader.consume(size, batch => {
      execute("START TRANSACTION;", false)
      batch.foreach(values => {
        var index = 1
        (anx.TYPES zip values).foreach{case (k, v) => {
          val obj: java.lang.Object = if(v=="NULL") k.zero else k.convert(v)
          k match {
            case `int`       => st.setInt(index, obj.asInstanceOf[java.lang.Integer])
            case `bigint`    => st.setLong(index, obj.asInstanceOf[java.lang.Long]) 
            case `bool`      => st.setBoolean(index, obj.asInstanceOf[java.lang.Boolean])
            case `double`    => st.setDouble(index, obj.asInstanceOf[java.lang.Double])
            case `timestamp` => st.setTimestamp(index, new Timestamp(obj.asInstanceOf[java.util.Date].getTime()))
            case `text`      => st.setString(index, obj.asInstanceOf[java.lang.String])
            case _ => println(s"Cannot convert $k:$v")
          }
          index += 1
        }}
        st.execute()//addBatch()
      })
      wt.time {execute("COMMIT;", false)}
    })
  }

  def init()(implicit con: Connection) {
    val rs: ResultSet = qt.time { con.createStatement().executeQuery(s"SELECT name FROM tables WHERE name like '${anx.TABLE_STD_FEED}'") }
    if(rs.next()) { 
      // if table exists then drop it
      qt.time { execute(s"DROP TABLE ${anx.TABLE_STD_FEED}", false)(con) } // reset
    }
    qt.time { execute(TABLE_SCHEMA, false)(con) } // init
  }

  def query()(implicit con: Connection) {
    qt.time { execute(s"SELECT COUNT(*) FROM ${anx.TABLE_STD_FEED}", true)(con) } // query
  }

  def execute(query: String, withResult: Boolean)(implicit con: Connection) {
    val st = con.createStatement()  
    if(!withResult) {
      st.execute(query)
      return
    }
    val rs: ResultSet = st.executeQuery(query)
    //if(rs.isAfterLast()) return // no rows 
    val md: ResultSetMetaData = rs.getMetaData()
    (1 to md.getColumnCount()).foreach(i => print(md.getColumnName(i) + ":" + md.getColumnTypeName(i) + "\t"))
    println("")  
    while (rs.next()) {
      (1 to md.getColumnCount()).foreach(j => println(rs.getString(j) + "\t"))
      println("")
    }
  }
  val TABLE_SCHEMA = s"CREATE TABLE ${anx.TABLE_STD_FEED} (key INT AUTO_INCREMENT PRIMARY KEY, auction_id_64 bigint, datetime timestamp, user_tz_offset int, width int, height int, media_type int, fold_position int, event_type text, imp_type int, payment_type int, media_cost_dollars_cpm double, revenue_type int, buyer_spend double, buyer_bid double, ecp double, eap double, is_imp int, is_learn int, predict_type_rev int, user_id_64 bigint, ip_address text, ip_address_trunc text, geo_country text, geo_region text, operating_system int, brower int, language int, venue_id int, seller_member_id int, publisher_id int, site_id int, site_domain text, tag_id int, external_inv_id int, reserve_price double, seller_revenue_cpm double, media_buy_rev_share_pct double, pub_rule_id int, seller_currency text, publisher_currency text, publisher_exchange_rate double, serving_fees_cpm double, serving_fees_revshare double, buyer_member_id int, advertiser_id int, brand_id int, advertiser_frequency int,  advertiser_recency int, insertion_order_id int, campaign_group_id int, campaign_id int, creative_id int, creative_freq int, creative_rec int, cadence_modifier double, can_convert int, user_group_id int, is_control int, controller_pct double, controller_creative_pct int, is_click int, pixel_id int, is_remarketing int, post_click_conv int, post_view_conv int, post_click_revenue double, post_view_revenue double, order_id text, external_data text, pricing_type text, booked_revenue_dollars double, booked_revenue_adv_curr double, commission_cpm double, commision_revshare double, auction_service_deduction double, auction_service_fees double, creative_overage_fees double, clear_fees double, buyer_currency text, advertiser_currency text, advertiser_exchange_rate double, latitude text, longitude text, device_unique_id text, device_id int, carrier_id int, deal_id int, view_result text, application_id text, supply_type text, sdk_version text, ozone_id int, billing_period_id int)"
}
