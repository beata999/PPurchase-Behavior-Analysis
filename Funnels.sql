SELECT * EXCEPT(row_num)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS row_num
  FROM `turing_data_analytics.raw_events`
)
WHERE row_num = 1 and event_name IN ('view_promotion', 'page_view', 'view_item', 'add_to_cart', 'add_payment_info', 'purchase');


Select Distinct event_name
from `turing_data_analytics.raw_events`
where event_name IN ('view_promotion', 'page_view', 'view_item', 'add_to_cart', 'add_payment_info', 'purchase')