# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,概要
# MAGIC %md
# MAGIC # SCM パイプライン検証
# MAGIC
# MAGIC パイプライン更新後に実行し、システム全体のデータ品質異常を検知する。
# MAGIC いずれかのチェックが失敗した場合、ジョブを失敗させる。
# MAGIC
# MAGIC **チェック項目:**
# MAGIC 1. Gold テーブルが空でないこと
# MAGIC 2. Silver 隔離率が 20% 未満であること（ソース異常検知）
# MAGIC 3. 集計整合性（Gold 合計 = Silver 有効完了注文の合計）

# COMMAND ----------

# DBTITLE 1,設定
# 設定
QUARANTINE_THRESHOLD = 0.20  # 隔離率の閾値
TOLERANCE = 0.01  # 集計比較の許容誤差

# カタログ名（ジョブパラメータで環境ごとに切替）
dbutils.widgets.text("catalog", "rongzi_catalog")
catalog = dbutils.widgets.get("catalog")

# テーブル参照（カタログはパラメータから動的に構築）
BRONZE_CUSTOMERS = f"{catalog}.scm_bronze.bronze_customers"
BRONZE_ORDERS = f"{catalog}.scm_bronze.bronze_orders"
SILVER_CUSTOMERS = f"{catalog}.scm_silver.silver_customers"
SILVER_ORDERS = f"{catalog}.scm_silver.silver_orders"
GOLD_SUMMARY = f"{catalog}.scm_gold.gold_customer_sales_summary"

print(f"検証対象カタログ: {catalog}")

# COMMAND ----------

# DBTITLE 1,チェック1: Gold非空
from pyspark.sql.functions import sum as _sum, col

results = []

# --- チェック 1: Gold テーブルが空でないこと ---
gold_count = spark.read.table(GOLD_SUMMARY).count()
check1_passed = gold_count > 0
results.append(("Gold テーブル非空", check1_passed, f"行数: {gold_count}"))

# COMMAND ----------

# DBTITLE 1,チェック2: 隔離率
# --- チェック 2: 隔離率が閾値未満であること ---
bronze_customers_count = spark.read.table(BRONZE_CUSTOMERS).count()
silver_customers_count = spark.read.table(SILVER_CUSTOMERS).count()
customers_quarantine_rate = 1 - (silver_customers_count / bronze_customers_count) if bronze_customers_count > 0 else 0

bronze_orders_count = spark.read.table(BRONZE_ORDERS).count()
silver_orders_count = spark.read.table(SILVER_ORDERS).count()
orders_quarantine_rate = 1 - (silver_orders_count / bronze_orders_count) if bronze_orders_count > 0 else 0

check2_passed = customers_quarantine_rate < QUARANTINE_THRESHOLD and orders_quarantine_rate < QUARANTINE_THRESHOLD
results.append(("隔離率 < 20%", check2_passed,
    f"顧客: {customers_quarantine_rate:.1%} ({bronze_customers_count} -> {silver_customers_count}), "
    f"注文: {orders_quarantine_rate:.1%} ({bronze_orders_count} -> {silver_orders_count})"))

# COMMAND ----------

# DBTITLE 1,チェック3: 集計整合性
# --- チェック 3: 集計整合性 ---
# Gold は Silver の有効顧客 × 完了注文の内部結合で構築されるため、
# 同じ条件で Silver 側を集計して比較する。
gold_total = (
    spark.read.table(GOLD_SUMMARY)
    .agg(_sum("total_amount"))
    .collect()[0][0] or 0
)

silver_valid_customers = spark.read.table(SILVER_CUSTOMERS).select("customer_id")
silver_total = (
    spark.read.table(SILVER_ORDERS)
    .filter(col("status") == "completed")
    .join(silver_valid_customers, "customer_id", "inner")
    .agg(_sum("amount"))
    .collect()[0][0] or 0
)

diff = abs(float(gold_total) - float(silver_total))
check3_passed = diff < TOLERANCE
results.append((
    "集計整合性",
    check3_passed,
    f"Gold: {gold_total}, Silver(有効顧客のみ): {silver_total}, 差分: {diff:.4f}"
))

# COMMAND ----------

# DBTITLE 1,一時: API呼び出し
# --- 最終結果 ---
print("=" * 60)
print("SCM パイプライン検証結果")
print("=" * 60)

failures = []
for name, passed, detail in results:
    status = "\u2713" if passed else "\u2717"
    print(f"  {status} {name}")
    print(f"    {detail}")
    if not passed:
        failures.append(f"{name}: {detail}")

print("=" * 60)

if failures:
    error_msg = "検証失敗:\n" + "\n".join(f"  - {f}" for f in failures)
    print(error_msg)
    raise Exception(error_msg)
else:
    print("全チェック合格")
