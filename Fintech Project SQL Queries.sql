/*

Queries used for Fintech Project

*/

-- 1. 

CREATE OR REPLACE TABLE `feisty-wall-358403.fin_tech_20231509.transactions`
AS
SELECT df.*,
      point_mechanism_rate,
      maximum_point_per_trans,
      CASE WHEN gmv*point_mechanism_rate < maximum_point_per_trans THEN gmv*point_mechanism_rate 
      ELSE maximum_point_per_trans END AS loyalty_points
FROM feisty-wall-358403.fin_tech_20231509.transactions_clean as df
LEFT JOIN feisty-wall-358403.fin_tech_20231509.LoyaltyPoints_data as loyaltypoints_data
USING(service_group)

-- 2. 
CREATE OR REPLACE TABLE `feisty-wall-358403.fin_tech_20231509.LoyaltyRanking`
AS 
SELECT order_date,user_id, sum(loyalty_points) as loyalty_points
FROM feisty-wall-358403.fin_tech_20231509.transactions
GROUP BY order_date, user_id

--3.
WITH get_expire_point AS(
SELECT 
DATE_ADD(DATE_ADD(order_date, INTERVAL 1 MONTH),INTERVAL 1 DAY) AS expire_date,
user_id,
- loyalty_points as expire_point
FROM feisty-wall-358403.fin_tech_20231509.LoyaltyRanking)
,point_data_v1 AS 
(SELECT *
FROM feisty-wall-358403.fin_tech_20231509.LoyaltyRanking
UNION ALL
SELECT *
FROM get_expire_point)
,point_data_v2 AS(
SELECT 
order_date,
user_id,
loyalty_points,
SUM(loyalty_points) OVER (PARTITION BY user_id ORDER BY order_date) AS calculated_point
FROM point_data_v1)

SELECT
order_date,
user_id,
loyalty_points,
calculated_point,
CASE 
  WHEN calculated_point >=5000 THEN 'DIAMOND' 
  WHEN calculated_point >=2000 THEN 'GOLD'
  WHEN calculated_point >=1000 THEN 'SILVER'
  ELSE 'STANDARD' END AS rank_name
FROM point_data_v2
WHERE loyalty_points >0
ORDER BY user_id, order_date

--4.
CREATE OR REPLACE TABLE `feisty-wall-358403.fin_tech_20231509.transactions_v2`
AS 
SELECT 
df1.*,
rank_name,
CASE 
 WHEN rank_name='DIAMOND' THEN 4 
 WHEN rank_name='GOLD' THEN 3
 WHEN rank_name='SILVER' THEN 2
 ELSE 1 END AS class_ID  
FROM feisty-wall-358403.fin_tech_20231509.transactions AS df1
LEFT JOIN feisty-wall-358403.fin_tech_20231509.loyaltyranking_v2 as df2
ON df1.user_id=df2.user_id AND df1.order_date=df2.order_date

# At the end of Mar 2022, how many user achieved rank Gold?
SELECT count(distinct user_id) AS Gold_users
FROM `feisty-wall-358403.fin_tech_20231509.loyaltyranking_v2`
WHERE user_id IN (
SELECT user_id
FROM `feisty-wall-358403.fin_tech_20231509.loyaltyranking_v2`
WHERE order_date BETWEEN '2022-03-01' AND '2022-03-31'
AND rank_name = 'GOLD')

--5.
SELECT 
df1.*, 
df2.cashback_rate,
CASE WHEN cashback_rate IS NULL THEN 0 
     WHEN GMV * cashback_rate/100 < 10000 THEN GMV * cashback_rate 
     ELSE 10000 END AS cashback_cost
FROM feisty-wall-358403.fin_tech_20231509.transactions_v2 AS df1
LEFT JOIN feisty-wall-358403.fin_tech_20231509.LoyaltyBenefits_data AS df2
ON df1.class_ID=df2.class_ID AND df1.service_group=df2.group

--6.
# Calculate the total cashback cost in February 2022.
SELECT SUM(cashback_cost) AS total_cashback_cost
FROM `feisty-wall-358403.fin_tech_20231509.transactions_v2` 
WHERE order_date BETWEEN '2022-02-01' AND '2022-02-28'
 
 --7.
# Users who can maintain a 20-day or longer streak of being in the DIAMOND ranking 
WITH get_diamond_user AS (
SELECT 
user_id AS diamond_users,
MIN(order_date) as early_order,
MAX(order_date) as final_order,
FROM `feisty-wall-358403.fin_tech_20231509.loyaltyranking_v2`
WHERE user_id IN (
SELECT user_id
FROM `feisty-wall-358403.fin_tech_20231509.loyaltyranking_v2`
WHERE order_date BETWEEN '2022-02-19' AND '2022-03-31'
AND rank_name = 'DIAMOND')
AND order_date BETWEEN '2022-02-19' AND '2022-03-31'
GROUP BY user_id)

SELECT 
diamond_users,
early_order,
final_order
FROM get_diamond_user
ORDER by final_order DESC






