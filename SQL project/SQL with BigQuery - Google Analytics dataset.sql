/*
Queries used for the Google Analytics sample dataset

*/

-- 1. 
 # Question 1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
 SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  SUM(totals.visits) AS total_visits,
  SUM(totals.pageviews) AS total_pageviews,
  SUM(totals.transactions) AS total_transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month

-- 2. 
# Question 2: Bounce rate per traffic source in July 2017
SELECT
  trafficSource.source AS source,
  COUNT(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  (SUM(totals.bounces) / COUNT(totals.visits)) * 100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC

--3.
# Question 3: Revenue by traffic source by week, by month in June 2017
SELECT 
  'Month' as time_type,
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
  trafficSource.source as source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
 WHERE _table_suffix BETWEEN '20170601' AND '20170630'
GROUP BY month, source 
UNION ALL
SELECT 
  'Week' as time_type,
  FORMAT_DATE("%Y%W",PARSE_DATE('%Y%m%d',date)) as week,
  trafficSource.source as source,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
 WHERE _table_suffix BETWEEN '20170601' AND '20170630'
GROUP BY week, source
ORDER BY revenue DESC
--4.
# Question 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
WITH purchase as (
SELECT
FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
AND totals.transactions >=1
group by month)

,nonpurchase as(
SELECT
FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_non_purchase
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
AND totals.transactions IS NULL
group by month)

SELECT
  purchase.month as month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM purchase
FULL JOIN nonpurchase
using (month)
order by month

--5.
# Question 5: Average number of transactions per user that made a purchase in July 2017
SELECT
format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
SUM(totals.transactions)/COUNT(distinct fullVisitorId) AS avg_total_transactions_per_user
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >=1
GROUP BY month

--6.
#Question 6: Average amount of money spent per session. Only include purchaser data in July 2017
SELECT
format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
(SUM(totals.totalTransactionRevenue)/sum(totals.visits))/1000000 AS avg_revenue_by_user_per_visit
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY month
 
 --7.
# Question 7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017
WITH product_data AS (
SELECT
    fullVisitorId,
    product.v2ProductName AS purchased_product,
    product.productQuantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
WHERE product.productRevenue IS NOT NULL)
,purchased_customers AS (
SELECT 
    DISTINCT fullVisitorId
FROM product_data
WHERE purchased_product = "YouTube Men's Vintage Henley")

SELECT
    purchased_product AS other_purchased_products,
    SUM(productQuantity) AS quantity
FROM product_data
WHERE
    fullVisitorId IN (SELECT fullVisitorId FROM purchased_customers)
    AND purchased_product <> "YouTube Men's Vintage Henley"
GROUP BY purchased_product
ORDER BY quantity DESC;

 --8.
 # Question 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month
WITH product_data AS(
SELECT
  format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) AS num_product_view,
  COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) AS num_addtocart,
  COUNT(CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL THEN product.v2ProductName END) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
GROUP BY month
ORDER BY month)

SELECT
  month,
  num_product_view,
  num_addtocart,
  num_purchase,
  ROUND(num_addtocart/num_product_view*100,2) AS add_to_cart_rate,
  ROUND(num_purchase/num_product_view*100,2) AS purchase_rate
FROM product_data




