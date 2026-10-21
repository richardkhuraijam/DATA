import pandas as pd

from data.hyderabad import ZONES
from tests.drills.broker_kill import decide
from tests.drills.spark_restart import verify_bronze


def test_broker_drill_fails_on_loss_duplicates_or_failures():
    assert decide(1000, 0, 1000, 1000)
    assert not decide(1000, 0, 998, 998)       # data loss
    assert not decide(1000, 0, 1003, 1000)     # duplicates in the log
    assert not decide(1000, 2, 1000, 1000)     # produce errors


def test_spark_restart_verifier_detects_gaps_and_partial_batches(tmp_path):
    def write(i, n=len(ZONES)):
        d = tmp_path / f"batch_id={i}"; d.mkdir(); pd.DataFrame({"zone_id": range(n)}).to_parquet(d / "p.parquet")
    for i in (4, 5, 6):
        write(i)
    assert verify_bronze(tmp_path)[0]
    write(8)                                    # batch 7 missing -> lost data
    assert not verify_bronze(tmp_path)[0]
    (tmp_path / "batch_id=8" / "p.parquet").unlink(); (tmp_path / "batch_id=8").rmdir(); write(7, n=3)
    assert not verify_bronze(tmp_path)[0]       # partial batch
