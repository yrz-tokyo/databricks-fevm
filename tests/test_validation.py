"""
ユニットテスト: パイプライン検証ロジック

GitHub Actions (CI) 上で pytest により実行される。
Spark/Databricks 環境に依存しない純粋な Python テスト。
"""
import sys
import os
import pytest

# src/ をインポートパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from utils.validation import (
    calc_quarantine_rate,
    check_quarantine_rate,
    check_aggregation_consistency,
)


# ============================================================
# calc_quarantine_rate
# ============================================================
class TestCalcQuarantineRate:
    def test_normal_case(self):
        """100件中17件隔離 → 17%"""
        assert calc_quarantine_rate(100, 83) == pytest.approx(0.17, abs=0.001)

    def test_no_quarantine(self):
        """隔離なし → 0%"""
        assert calc_quarantine_rate(100, 100) == 0.0

    def test_all_quarantined(self):
        """全件隔離 → 100%"""
        assert calc_quarantine_rate(100, 0) == 1.0

    def test_bronze_zero(self):
        """Bronze が空 → 0.0（ゼロ除算回避）"""
        assert calc_quarantine_rate(0, 0) == 0.0

    def test_bronze_negative(self):
        """Bronze が負（異常値）→ 0.0"""
        assert calc_quarantine_rate(-1, 50) == 0.0


# ============================================================
# check_quarantine_rate
# ============================================================
class TestCheckQuarantineRate:
    def test_below_threshold(self):
        """閾値未満 → True（正常）"""
        assert check_quarantine_rate(0.15, 0.20) is True

    def test_at_threshold(self):
        """ちょうど閾値 → False（異常）"""
        assert check_quarantine_rate(0.20, 0.20) is False

    def test_above_threshold(self):
        """閾値超過 → False（異常）"""
        assert check_quarantine_rate(0.25, 0.20) is False

    def test_zero_rate(self):
        """隔離なし → True"""
        assert check_quarantine_rate(0.0, 0.20) is True


# ============================================================
# check_aggregation_consistency
# ============================================================
class TestCheckAggregationConsistency:
    def test_exact_match(self):
        """完全一致 → True"""
        assert check_aggregation_consistency(688144.46, 688144.46) is True

    def test_within_tolerance(self):
        """誤差 0.005 < tolerance 0.01 → True"""
        assert check_aggregation_consistency(1000.000, 1000.005, 0.01) is True

    def test_exceeds_tolerance_clearly(self):
        """差分 0.02 > tolerance 0.01 → False"""
        assert check_aggregation_consistency(1000.00, 1000.02, 0.01) is False

    def test_large_difference(self):
        """大きな差分 → False"""
        assert check_aggregation_consistency(1000.00, 1001.00, 0.01) is False

    def test_zero_totals(self):
        """両方ゼロ → True"""
        assert check_aggregation_consistency(0, 0) is True

    def test_custom_tolerance(self):
        """カスタム許容誤差"""
        assert check_aggregation_consistency(100.0, 100.5, tolerance=1.0) is True
