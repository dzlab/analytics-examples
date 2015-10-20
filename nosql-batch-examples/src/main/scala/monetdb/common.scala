package monetdb

import common._

object monetdb {

  val TABLE_SCHEMA = s"CREATE TABLE ${anx.TABLE_STD_FEED} (key INT AUTO_INCREMENT PRIMARY KEY, auction_id_64 bigint, datetime timestamp, user_tz_offset int, width int, height int, media_type int, fold_position int, event_type text, imp_type int, payment_type int, media_cost_dollars_cpm double, revenue_type int, buyer_spend double, buyer_bid double, ecp double, eap double, is_imp int, is_learn int, predict_type_rev int, user_id_64 bigint, ip_address text, ip_address_trunc text, geo_country text, geo_region text, operating_system int, brower int, language int, venue_id int, seller_member_id int, publisher_id int, site_id int, site_domain text, tag_id int, external_inv_id int, reserve_price double, seller_revenue_cpm double, media_buy_rev_share_pct double, pub_rule_id int, seller_currency text, publisher_currency text, publisher_exchange_rate double, serving_fees_cpm double, serving_fees_revshare double, buyer_member_id int, advertiser_id int, brand_id int, advertiser_frequency int,  advertiser_recency int, insertion_order_id int, campaign_group_id int, campaign_id int, creative_id int, creative_freq int, creative_rec int, cadence_modifier double, can_convert int, user_group_id int, is_control int, controller_pct double, controller_creative_pct int, is_click int, pixel_id int, is_remarketing int, post_click_conv int, post_view_conv int, post_click_revenue double, post_view_revenue double, order_id text, external_data text, pricing_type text, booked_revenue_dollars double, booked_revenue_adv_curr double, commission_cpm double, commision_revshare double, auction_service_deduction double, auction_service_fees double, creative_overage_fees double, clear_fees double, buyer_currency text, advertiser_currency text, advertiser_exchange_rate double, latitude text, longitude text, device_unique_id text, device_id int, carrier_id int, deal_id int, view_result text, application_id text, supply_type text, sdk_version text, ozone_id int, billing_period_id int)"
 
}
