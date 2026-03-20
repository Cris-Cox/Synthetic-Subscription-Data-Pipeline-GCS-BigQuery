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
  WHERE paid = true
    AND transaction_id IS NOT NULL
    AND subscription_name IN ('Basic monthly', 'Basic annual', 'Pro monthly', 'Pro annual')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY created_at) = 1
),

subscription_metrics AS (
  SELECT
    subscription_month,
    subscription_name,
    COUNT(DISTINCT invoice_id) AS total_subscriptions,
    COUNTIF(subscription_number = 1) AS new_subscriptions,
    COUNTIF(subscription_number > 1) AS renewals,
    SUM(revenue) AS total_revenue,
    SUM(CASE WHEN subscription_number = 1 THEN revenue ELSE 0 END) AS new_subscription_revenue,
    SUM(CASE WHEN subscription_number > 1 THEN revenue ELSE 0 END) AS renewal_revenue
  FROM ranked_subscriptions
  GROUP BY subscription_month, subscription_name
),

latest_customer_subscription AS (
  SELECT
    customer_id,
    subscription_name,
    created_at AS last_subscription_date,
    revenue,
    DATE_TRUNC(created_at, MONTH) AS last_subscription_month
  FROM (
    SELECT
      customer_id,
      subscription_name,
      created_at,
      revenue,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
    FROM ranked_subscriptions
  )
  WHERE rn = 1
),

churned_customers AS (
  SELECT
    last_subscription_month AS subscription_month,
    subscription_name,
    COUNT(DISTINCT customer_id) AS churned_customers,
    SUM(revenue) AS lost_revenue
  FROM latest_customer_subscription
  WHERE
    (
      subscription_name LIKE '%monthly%'
      AND DATE(last_subscription_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    )
    OR
    (
      subscription_name LIKE '%annual%'
      AND DATE(last_subscription_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    )
  GROUP BY last_subscription_month, subscription_name
)

SELECT
  m.subscription_month,
  m.subscription_name,
  m.total_subscriptions AS `Total Subscriptions`,
  m.new_subscriptions AS `New Subscriptions`,
  m.renewals AS `Renewals`,
  m.total_revenue AS `Total Revenue`,
  m.new_subscription_revenue AS `New Subscription Revenue`,
  m.renewal_revenue AS `Renewal Revenue`,
  COALESCE(s.churned_customers, 0) AS `Stopped Customers`,
  COALESCE(s.lost_revenue, 0) AS `Stopped Revenue`
FROM subscription_metrics m
LEFT JOIN churned_customers s
  ON m.subscription_month = s.subscription_month
  AND m.subscription_name = s.subscription_name
ORDER BY m.subscription_month DESC, m.total_revenue DESC
