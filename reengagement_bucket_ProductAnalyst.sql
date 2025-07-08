WITH purchases AS (
  SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
    total_item_quantity,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY PARSE_DATE('%Y%m%d', event_date)
    ) AS rn
  FROM `turing_data_analytics.raw_events`
  WHERE event_name = "purchase"
    AND total_item_quantity IS NOT NULL
),

paired AS (
  SELECT
    curr.user_pseudo_id,
    DATE_DIFF(curr.purchase_date, prev.purchase_date, DAY) AS days_between
  FROM purchases curr
  LEFT JOIN purchases prev
    ON curr.user_pseudo_id = prev.user_pseudo_id
    AND curr.rn = prev.rn + 1
  WHERE prev.purchase_date IS NOT NULL
),

-- Create histogram buckets
bucketed AS (
  SELECT
    CASE
      WHEN days_between BETWEEN 0 AND 7 THEN '0–7 days'
      WHEN days_between BETWEEN 8 AND 14 THEN '8–14 days'
      WHEN days_between BETWEEN 15 AND 30 THEN '15–30 days'
      WHEN days_between BETWEEN 31 AND 60 THEN '31–60 days'
      WHEN days_between BETWEEN 61 AND 90 THEN '61–90 days'
      WHEN days_between BETWEEN 91 AND 180 THEN '91–180 days'
      WHEN days_between > 180 THEN '180+ days'
      ELSE 'Unknown'
    END AS reengagement_bucket
  FROM paired
)

-- Count how many intervals fall into each bucket
SELECT 
  reengagement_bucket,
  COUNT(*) AS num_intervals
FROM bucketed
GROUP BY reengagement_bucket
ORDER BY 
  CASE 
    WHEN reengagement_bucket = '0–7 days' THEN 1
    WHEN reengagement_bucket = '8–14 days' THEN 2
    WHEN reengagement_bucket = '15–30 days' THEN 3
    WHEN reengagement_bucket = '31–60 days' THEN 4
    WHEN reengagement_bucket = '61–90 days' THEN 5
    WHEN reengagement_bucket = '91–180 days' THEN 6
    WHEN reengagement_bucket = '180+ days' THEN 7
    ELSE 8
  END
