SELECT
 user_pseudo_id,
  FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS purchase_date,
  country,
  category,
  MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_session_time,
  COUNT(total_item_quantity) AS total_item_quantity,
  SUM(purchase_revenue_in_usd) AS revenue_usd
FROM
  `turing_data_analytics.raw_events`
WHERE
  total_item_quantity IS NOT NULL
  GROUP BY 1,2, 3, 4