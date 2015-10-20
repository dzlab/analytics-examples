package druid

object Sample {
  def main(args: Array[String]) {
    val start = System.nanoTime
    //ingest_std_feed() 
    query_std_feed() 
    val end = System.nanoTime
    println(s"In ${(end-start)/1e6} ms")
  }

  def query_group_by() {
    val aggregas = List(Aggregation("doubleSum", "revenue", "revenue"))
    val query = GroupByQuery("revenue_impressions", "hour", List("buyer_id"), None, None, aggregas, List(), List("2015-08-01/2015-09-01"), None)
    Utils.echo(query.toJson) 
    HttpUtils.doPost("localhost", 8082, "druid/v2/?pretty", Utils.stringfy(query.toJson))
  }

  /**
   * Query data in druid
   * */
  def query_std_feed() = {
     val aggregas = List(Aggregation("doubleSum", "seller_revenue_cpm", "seller_revenue_cpm"))
    //val filter = NotFilter(SelectorFilter("buyer_member_id", null))
    val query2 = GroupByQuery("994_2015_08_12_12_standard_feed", "hour", List(), None, None, List(Aggregation("count", "rows", "rows")), List(), List("2015-08-01/2015-09-01"), None)
    val query1 = GroupByQuery("994_2015_08_12_12_standard_feed", "hour", List("buyer_member_id", "brand_id"), None, None, aggregas, List(), List("2015-08-01/2015-09-01"), None)
    //DruidUtils.echo(query.toJson) 
    
    HttpUtils.doPost("localhost", 8082, "druid/v2/?pretty", Utils.stringfy(query1.toJson))
    HttpUtils.doPost("localhost", 8082, "druid/v2/?pretty", Utils.stringfy(query2.toJson))
  }

  /**
   * ingest data into druid
   * */
  def ingest_std_feed() = {
    val granularity = GranularitySpec("uniform", "HOUR", "NONE", List("2015-08-01/2015-09-01"))
  
    val columns = List("auction_id_64", "datetime", "user_tz_offset", "width", "height", "media_type", "fold_position", "event_type", "imp_type", "payment_type", "media_cost_dollars_cpm", "revenue_type", "buyer_spend", "buyer-bid", "ecp", "eap", "is_imp", "is_learn", "predict_type_rev", "user_id_64", "ip_address", "ip_address_trunc", "geo_country", "geo_region", "operating_system", "brower", "language", "venue_id", "seller_member_id", "publisher_id", "site_id", "site_domain", "tag_id", "external_inv_id", "reserve_price", "seller_revenue_cpm", "media_buy_rev_share_pct", "pub_rule_id", "seller_currency", "publisher_currency", "publisher_exchange_rate", "serving_fees_cpm", "serving_fees_revshare", "buyer_member_id", "advertiser_id", "brand_id", "advertiser_frequency",  "advertiser_recency", "insertion_order_id", "campaign_group_id", "campaign_id", "creative_id", "creative_freq", "creative_rec", "cadence_modifier", "can_convert", "user_group_id", "is_control", "controller_pct", "controller_creative_pct", "is_click", "pixel_id", "is_remarketing", "post_click_conv", "post_view_conv", "post_click_revenue", "post_view_revenue", "order_id", "external_data", "pricing_type", "booked_revenue_dollars", "booked_revenue_adv_curr", "commission_cpm", "commision_revshare", "auction_service_deduction", "auction_service_fees", "creative_overage_fees", "clear_fees", "buyer_currency", "advertiser_currency", "advertiser_exchange_rate", "latitude", "longitude", "device_unique_id", "device_id", "carrier_id", "deal_id", "view_result", "application_id", "supply_type", "sdk_version", "ozone_id", "billing_period_id") 
  
    val dimensions = List("auction_id_64", "media_type", "event_type", "imp_type", "payment_type", "revenue_type", "user_id_64", "buyer_member_id",  "advertiser_id", "brand_id", "campaign_id", "creative_id", "order_id", "pricing_type", "device_id", "carrier_id", "deal_id", "is_click")
    val metricColumns = List("buyer-bid", "seller_revenue_cpm")
 
    val metrics = metricColumns.map(m => Aggregation("doubleSum", m, m))

    val parseSpec = TSVParseSpec("tsv", TimestampSpec("datetime", "yyyy-MM-dd HH:mm:ss"), DimensionsSpec(dimensions, List(), List()), None, None, columns)
    val parser = Parser("string", parseSpec)

    val schema = DataSchema("994_2015_08_12_12_standard_feed", parser, metrics, granularity)
    val io = IOConfig("index", "local", "/Users/bachir/Workspace/DB/data/files", "*_standard_feed_*.csv")
    val tuning = TuningConfig("index", 0, 0)

    val task = IndexTask("index", schema, io, tuning)
    val taskStr = Utils.stringfy(task.toJson)

    HttpUtils.doPost("localhost", 8090, "druid/indexer/v1/task", taskStr)

  }

}
