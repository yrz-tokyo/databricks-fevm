-- SCM DataOps 監視クエリ集
-- ダッシュボード「SCM DataOps Monitoring」のデータソース

-- 1. ジョブ実行履歴（直近30日）
SELECT DATE(r.period_start_time) AS run_date,
       r.result_state,
       COUNT(*) AS run_count
FROM system.lakeflow.job_run_timeline r
INNER JOIN (SELECT DISTINCT job_id, name FROM system.lakeflow.jobs WHERE name LIKE '%SCM%') j
  ON r.job_id = j.job_id
WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND r.result_state IS NOT NULL
GROUP BY run_date, r.result_state
ORDER BY run_date;

-- 2. タスク別実行時間トレンド
SELECT DATE(t.period_start_time) AS run_date,
       t.task_key,
       t.execution_duration_seconds
FROM system.lakeflow.job_task_run_timeline t
INNER JOIN (SELECT DISTINCT job_id FROM system.lakeflow.jobs WHERE name LIKE '%SCM%Orchestration%') j
  ON t.job_id = j.job_id
WHERE t.period_start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND t.execution_duration_seconds IS NOT NULL
ORDER BY run_date;

-- 3. DBU消費量（直近30日）
SELECT usage_date,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
  AND (usage_metadata.job_id IN ('230981639561573', '967703797599374', '110000659696310')
       OR usage_metadata.dlt_pipeline_id IN ('b96f6f9b-db72-4458-88e9-b7738eb8fe90'))
GROUP BY usage_date
ORDER BY usage_date;

-- 4. パイプライン更新履歴
SELECT DATE(t.period_start_time) AS update_date,
       t.result_state,
       COUNT(*) AS update_count
FROM system.lakeflow.pipeline_update_timeline t
INNER JOIN (SELECT DISTINCT pipeline_id FROM system.lakeflow.pipelines WHERE name LIKE '%SCM%') p
  ON t.pipeline_id = p.pipeline_id
WHERE t.period_start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND t.result_state IS NOT NULL
GROUP BY update_date, t.result_state
ORDER BY update_date;
