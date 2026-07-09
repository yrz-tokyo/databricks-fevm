-- Silver層 - データクレンジング・重複排除・品質ルール適用
-- Bronzeテーブルから読み込み、品質ルールを適用して整形する

CREATE OR REFRESH MATERIALIZED VIEW scm_silver.silver_customers (
  CONSTRAINT valid_email EXPECT (email IS NOT NULL AND email != '') ON VIOLATION DROP ROW
)
COMMENT 'クレンジング済み顧客データ - 重複排除、セグメント正規化、有効メールのみ'
AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS _row_num
  FROM scm_bronze.bronze_customers
)
SELECT
  customer_id,
  name,
  email,
  INITCAP(TRIM(segment)) AS segment,
  _source_file,
  _ingested_at
FROM ranked
WHERE _row_num = 1;


CREATE OR REFRESH MATERIALIZED VIEW scm_silver.silver_orders (
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('completed', 'pending', 'cancelled'))
)
COMMENT 'クレンジング済み注文データ - 重複排除、正の金額のみ、適切な型変換'
AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) AS _row_num
  FROM scm_bronze.bronze_orders
)
SELECT
  order_id,
  customer_id,
  CAST(amount AS DECIMAL(10,2)) AS amount,
  status,
  CAST(order_date AS DATE) AS order_date,
  _source_file,
  _ingested_at
FROM ranked
WHERE _row_num = 1;
