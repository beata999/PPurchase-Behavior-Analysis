SELECT 
    SUM(purchase_revenue_in_usd) / COUNT(*) AS AOV
  FROM 
    `tc-da-1.turing_data_analytics.raw_events`
  WHERE 
    event_name = 'purchase'