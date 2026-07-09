-- Gold層 - ビジネスレベルの集計
-- Silver顧客とSilver注文を結合し、顧客別売上サマリを生成する

CREATE OR REFRESH MATERIALIZED VIEW scm_gold.gold_customer_sales_summary
COMMENT '顧客別売上サマリ - 合計金額、注文件数、平均注文額、最終注文日'
AS
SELECT
  c.customer_id,
  c.name,
  c.email,
  c.segment,
  SUM(o.amount) AS total_amount,
  COUNT(o.order_id) AS order_count,
  AVG(o.amount) AS avg_order_amount,
  MAX(o.order_date) AS last_order_date
FROM scm_silver.silver_customers c
INNER JOIN scm_silver.silver_orders o
  ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY
  c.customer_id,
  c.name,
  c.email,
  c.segment;
