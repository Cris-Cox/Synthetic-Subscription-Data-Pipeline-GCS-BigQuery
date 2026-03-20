WITH ranked_subscriptions AS (
  SELECT
    customer_id,
    invoice_id,
    created_at,
    subscription_name,
    paid,
    CAST(net_amount_cents / 100 AS INT64) AS revenue,
    DATE_TRUNC(created_at, MONTH) AS subscription_month,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) AS subscription_number
  FROM `portfolio-479002.subscriptions.subscriptions`
  WHERE paid = true AND transaction_id is not null
  QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY created_at) = 1
)

SELECT
  subscription_month,
  subscription_name,
  COUNT(DISTINCT invoice_id) AS `Total Subscriptions`,
  COUNTIF(subscription_number = 1) AS `New Subscriptions`,
  COUNTIF(subscription_number > 1) AS Renewals,
  SUM(revenue) AS `Total Revenue`,
  SUM(CASE WHEN subscription_number = 1 THEN revenue ELSE 0 END) AS `New Subscription Revenue`,
  SUM(CASE WHEN subscription_number > 1 THEN revenue ELSE 0 END) AS `Renewal Revenue`
FROM ranked_subscriptions
WHERE subscription_name IN ('Basic monthly', 'Basic annual', 'Pro monthly', 'Pro annual')
GROUP BY subscription_month, subscription_name
ORDER BY subscription_month DESC, `Total Revenue` DESC
