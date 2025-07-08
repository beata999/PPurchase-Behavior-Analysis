  --how long it sesion take
  WITH first_sessions AS (
    SELECT
    user_pseudo_id,
    FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS session_date,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_session_time
  FROM
    `turing_data_analytics.raw_events`
  WHERE
    event_name = 'session_start'
  GROUP BY
    user_pseudo_id, session_date
    ),
    first_purchases AS (
  SELECT
    user_pseudo_id,
    FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS purchase_date,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_purchase_time
  FROM
    `turing_data_analytics.raw_events`
  WHERE
    event_name = 'purchase'
  GROUP BY
    user_pseudo_id, purchase_date
),
joined_data AS (
  SELECT
    s.user_pseudo_id,
    s.session_date AS date,
    s.first_session_time,
    p.first_purchase_time,
    TIMESTAMP_DIFF(p.first_purchase_time, s.first_session_time, SECOND) AS seconds_to_purchase
  FROM
    first_sessions s
  JOIN
    first_purchases p
  ON
    s.user_pseudo_id = p.user_pseudo_id AND s.session_date = p.purchase_date
)
SELECT
  date,
  COUNT(*) AS users_with_purchase,
  ROUND(AVG(seconds_to_purchase), 2) AS avg_seconds_to_purchase
FROM
  joined_data
GROUP BY
  date
ORDER BY
  date;

