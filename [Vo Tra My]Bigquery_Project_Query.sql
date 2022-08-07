-- Big project for SQL
-- Link sample: https://console.cloud.google.com/bigquery?project=ecommerce-349412&ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL
select
  left(date,6) as month, 
  sum(totals.visits) as total_visits, 
  sum(totals.pageviews) as total_pageviews,
  sum(totals.transactions) as total_transactions,
  sum(totals.totaltransactionrevenue)/power(10,6) as total_revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170101' and '20170331'
group by month
order by month;


-- Query 02: Bounce rate per traffic source in July 2017

#standardSQL
select
  trafficsource.source as source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  sum(totals.bounces)/sum(totals.visits)*100 as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017

#standardSQL
with month_data as (
  select
    "Month" as time_type,
    left(date,6) as time,
    trafficsource.source,
    sum(totals.totaltransactionrevenue)/power(10,6) as revenue
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time, trafficsource.source
),
  
  week_data as (
  select
    "Week" as time_type,
    format_date("%Y%W",parse_date("%Y%m%d",date)) as time,
    trafficsource.source,
    sum(totals.totaltransactionrevenue)/power(10,6) as revenue
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time, trafficsource.source
)

select * from month_data
union all
select * from week_data
order by revenue desc;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 
Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

#standardSQL
with purchaser_data as (
  select
    left(date,6) as month,
    fullvisitorid,
    sum(totals.pageviews) as purchaser_total_pageviews
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  Where _table_suffix between '20170601' and '20170731'
    and totals.transactions > 0
  group by month, fullvisitorid
),
  
  nonpurchaser_data as ( 
  select
    left(date,6) as month,
    fullvisitorid,
    sum(totals.pageviews) as nonpurchaser_total_pageviews,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  Where _table_suffix between '20170601' and '20170731'
    and totals.transactions is null
  group by month, fullvisitorid
)

select
  np.month,
  avg(purchaser_total_pageviews) as avg_pageviews_purchasers,
  avg(nonpurchaser_total_pageviews) as avg_pageviews_non_purchasers
from purchaser_data p
join nonpurchaser_data np using (month)
group by np.month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017

#standardSQL
with transactions_per_user as (
  select
    left(date,6) as month,
    fullvisitorid,
    sum(totals.transactions) as total_transactions
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  where totals.transactions is not null
  group by month, fullvisitorid
)

select
  month,
  avg(total_transactions) as avg_total_transactions_per_user
from transactions_per_user
group by month;


--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017

#standardSQL
with per_visitor_data as (
  select
    left(date,6) as month,
    fullvisitorid,
    sum(totals.totaltransactionrevenue) as total_revenue,
    sum(totals.visits) as total_visits
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  where totals.transactions is not null
  group by month, fullvisitorid
)

select 
  month,
  round(sum(total_revenue)/sum(total_visits),2) as avg_revenue_by_user_per_visit
from per_visitor_data
group by month;


--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

#standardSQL
with specific_visitor as (
  select
    distinct fullvisitorid
  from
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest (hits) as hits,
    unnest (hits.product) as product
  where product.v2ProductName="YouTube Men's Vintage Henley"
    and product.productrevenue is not null
)

select
  product.v2ProductName as other_purchased_products,
  sum(product.productquantity) as quantity
from 
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest (hits) as hits,
  unnest (hits.product) as product
inner join specific_visitor sv using (fullvisitorid)
where product.productrevenue is not null 
  and product.v2ProductName <> "YouTube Men's Vintage Henley"
group by product.v2ProductName
order by sum(product.productquantity) desc;


--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
 
#standardSQL
with product_view as (
  select
    left(date,6) as month,
    count(product.productSKU) as num_product_view
  from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest (hits) as hits,
    unnest (hits.product) as product
  where _table_suffix between '20170101' and '20170331'
    and hits.eCommerceAction.action_type = '2'
  group by month
),

  addtocart as (
  select
    left(date,6) as month,
    count(product.productSKU) as num_addtocart
  from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest (hits) as hits,
    unnest (hits.product) as product
  where _table_suffix between '20170101' and '20170331'
    and hits.eCommerceAction.action_type = '3'
  group by month
),

    purchase as (
  select
    left(date,6) as month,
    count(product.productSKU) as num_purchase
  from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest (hits) as hits,
    unnest (hits.product) as product
  where _table_suffix between '20170101' and '20170331'
    and hits.eCommerceAction.action_type = '6'
  group by month
)

select
  pv.month,
  num_product_view,
  num_addtocart,
  num_purchase,
  round(num_addtocart/num_product_view*100,2) as add_to_cart_rate,
  round(num_purchase/num_product_view*100,2) as purchase_rate
from product_view pv
join addtocart a on pv.month = a.month 
join purchase p on pv.month = p.month
order by pv.month;
