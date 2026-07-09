"""
パイプライン検証ロジック（純関数）

ノートブック validate_pipeline.py から呼び出される。
Spark に依存しない純粋な Python ロジックのみ抽出し、
GitHub Actions 上で pytest によるユニットテストを可能にする。
"""


def calc_quarantine_rate(bronze_count: int, silver_count: int) -> float:
    """Bronze→Silver で隔離（除外）された行の割合を計算する。

    Args:
        bronze_count: Bronze テーブルの行数
        silver_count: Silver テーブルの行数（品質ルール適用後）

    Returns:
        隔離率（0.0〜1.0）。bronze_count が 0 の場合は 0.0。
    """
    if bronze_count <= 0:
        return 0.0
    return 1 - (silver_count / bronze_count)


def check_quarantine_rate(rate: float, threshold: float) -> bool:
    """隔離率が閾値未満であることを確認する。

    Args:
        rate: 隔離率（calc_quarantine_rate の戻り値）
        threshold: 閾値（例: 0.20 = 20%）

    Returns:
        True なら正常（閾値未満）、False なら異常。
    """
    return rate < threshold


def check_aggregation_consistency(
    gold_total: float, silver_total: float, tolerance: float = 0.01
) -> bool:
    """Gold と Silver の集計値が許容誤差内で一致するか確認する。

    Args:
        gold_total: Gold テーブルの合計金額
        silver_total: Silver テーブルの有効完了注文の合計金額
        tolerance: 許容誤差（デフォルト 0.01）

    Returns:
        True なら整合（差分が tolerance 未満）、False なら不整合。
    """
    diff = abs(float(gold_total) - float(silver_total))
    return diff < tolerance
