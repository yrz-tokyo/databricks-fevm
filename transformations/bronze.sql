-- Bronze層 - Auto Loaderによる生データ増分取り込み
-- Volume のCSVファイルを読み込み、ソースファイル名と取り込み時刻を付与する
-- ${catalog_name} はパイプライン設定の configuration から注入される（DAB変数経由）

CREATE OR REFRESH STREAMING TABLE scm_bronze.bronze_customers
COMMENT 'CSVファイルからAuto Loaderで取り込んだ生の顧客データ'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/scm_volume/input_data/customers_*.csv',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE scm_bronze.bronze_orders
COMMENT 'CSVファイルからAuto Loaderで取り込んだ生の注文データ'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/${catalog_name}/scm_volume/input_data/orders_*.csv',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);
