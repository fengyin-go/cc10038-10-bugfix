"""重复写入修复后的解析数据流验收测试。

覆盖验收路径：
- 同规则并行解析两次三行日志：明细自然键唯一、第二次全部复用、原始行引用不翻倍；
- 切换候选规则版本重跑：新版本独立成单、旧版本依据保留；
- 并发提交：批次锁保证只有一套明细进入下游，回执区分已确认与运行中尝试；
- 采集血缘以唯一明细建立用量统计来源，并回写批次/数据源来源清单；
- 统计明细可逐行溯源原文，历史重复残留可标记清理且不污染统计。
"""
from __future__ import annotations

import json
import threading

from app.store import store


THREE_LOGS = [
    {"body": "2026-01-15T10:30:00|INFO|server started"},
    {"body": "2026-01-15T10:31:00|WARN|memory high"},
    {"body": "2026-01-15T10:32:00|ERROR|connection timeout"},
]


def _prepare_batch(pipeline, source_id="dedup-fix-src", logs=None):
    pipeline.register_source(source_id, "http", "default")
    pipeline.validate_source(source_id)
    result = pipeline.start_ingest_batch(source_id, "2026-01-15", logs or THREE_LOGS)
    return source_id, result["batch"]["批次编号"]


def _batch_entities(batch_no, rule_key=None, include_superseded=True):
    rows = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
    if rule_key is not None:
        rows = [
            p for p in rows
            if p.get("rule_version_key", p.get("解析规则版本")) == rule_key
        ]
    if not include_superseded:
        rows = [p for p in rows if not p.get("superseded")]
    return rows


class TestIdempotentParse:
    """同一批次同一规则版本重复解析：明细唯一、引用不翻倍。"""

    def test_same_rule_parsed_twice_yields_unique_entities(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule(
            "candidate-rule", "dedup-fix-src", output_fields=["time", "level", "message"],
            version="v1",
        )
        rule_key = "candidate-rule@v1"

        first = pipeline.parse_batch(batch_no, rule_key, operator="alice")
        second = pipeline.parse_batch(batch_no, rule_key, operator="bob")

        assert first["ok"] is True
        assert first["state"] == "已确认"
        assert first["created"] == 3
        assert first["reused"] == 0
        assert second["created"] == 0
        assert second["reused"] == 3

        entities = _batch_entities(batch_no, rule_key)
        assert len(entities) == 3
        natural_keys = [e["natural_key"] for e in entities]
        assert len(set(natural_keys)) == 3
        # 每个自然键都是 原始行 + 规则版本
        for raw in store.rows("raw_records"):
            if raw.get("批次编号") == batch_no:
                assert f"{raw['id']}|{rule_key}" in natural_keys

    def test_receipt_contains_version_usage_and_lineage(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("r-a", "dedup-fix-src", version="v1")
        receipt = pipeline.parse_batch(batch_no, "r-a@v1", operator="alice")

        assert receipt["rule_version_key"] == "r-a@v1"
        assert receipt["schema_version"]
        assert receipt["lineage_key"] == f"{batch_no}|r-a@v1"
        # 明细用量：成功数与原始行数一致
        assert receipt["success"] == 3
        assert receipt["failure"] == 0
        assert len(receipt["parsed_ids"]) == 3
        assert len(receipt["raw_refs"]) == 3
        # 血缘关联
        lineage = store.find("data_lineage", receipt["lineage_id"])
        assert json.loads(lineage["entity_ids"]) == sorted(receipt["parsed_ids"])
        assert lineage["record_count"] == 3

    def test_raw_row_refs_not_duplicated_after_reparse(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("r-b", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "r-b@v1")
        pipeline.parse_batch(batch_no, "r-b@v1")
        pipeline.parse_batch(batch_no, "r-b@v1")

        for raw in store.rows("raw_records"):
            if raw.get("批次编号") != batch_no:
                continue
            refs = json.loads(raw["parsed_refs"])
            assert len(refs) == 1
            assert refs[0]["rule_version_key"] == "r-b@v1"
            assert raw["解析状态"] == "已解析"

    def test_batch_parsed_ids_stay_unique_across_submissions(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("r-c", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "r-c@v1")
        pipeline.parse_batch(batch_no, "r-c@v1")

        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == batch_no)
        parsed_ids = json.loads(batch["parsed_ids"])
        assert len(parsed_ids) == 3
        assert len(set(parsed_ids)) == 3

    def test_journal_records_every_submission_without_duplicate_entities(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("r-d", "dedup-fix-src", version="v1")
        before = len(store.rows("parse_journal"))
        pipeline.parse_batch(batch_no, "r-d@v1", operator="alice")
        pipeline.parse_batch(batch_no, "r-d@v1", operator="bob")
        journals = [
            j for j in store.rows("parse_journal")[before:]
            if j.get("批次编号") == batch_no and j.get("操作类型") == "批量解析"
        ]
        assert len(journals) == 2
        assert all(j["attempt_state"] == "已确认" for j in journals)
        assert journals[1]["reused_count"] == "3"
        # 操作日志有两条，但明细仍是唯一一套
        assert len(_batch_entities(batch_no, "r-d@v1")) == 3


class TestRuleVersionSwitch:
    """切换候选规则版本重跑：新版本独立解释原文，旧版本保留依据。"""

    def test_new_rule_version_creates_independent_entities(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule(
            "evolve-rule", "dedup-fix-src",
            output_fields=["time", "level", "message"], version="v1",
        )
        pipeline.publish_parser_rule(
            "evolve-rule", "dedup-fix-src",
            output_fields=["ts", "lvl", "msg"], version="v2",
        )

        r1 = pipeline.parse_batch(batch_no, "evolve-rule@v1")
        r2 = pipeline.parse_batch(batch_no, "evolve-rule@v2")

        assert r1["created"] == 3 and r2["created"] == 3
        v1_entities = _batch_entities(batch_no, "evolve-rule@v1")
        v2_entities = _batch_entities(batch_no, "evolve-rule@v2")
        assert len(v1_entities) == 3 and len(v2_entities) == 3
        # 同一原始行在两个版本下各有一条独立明细
        raw_id = v1_entities[0]["原始记录ID"]
        keys = {
            e["natural_key"]
            for e in store.rows("parsed_entity")
            if e["原始记录ID"] == raw_id
        }
        assert keys == {f"{raw_id}|evolve-rule@v1", f"{raw_id}|evolve-rule@v2"}
        # 两版字段解释不同
        v1_fields = json.loads(v1_entities[0]["标准化字段集"])
        v2_fields = json.loads(v2_entities[0]["标准化字段集"])
        assert "time" in v1_fields and "ts" in v2_fields

    def test_old_version_entities_and_lineage_remain_after_rerun(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("keep-rule", "dedup-fix-src", version="v1")
        pipeline.publish_parser_rule("keep-rule", "dedup-fix-src", version="v2")
        pipeline.parse_batch(batch_no, "keep-rule@v1")
        pipeline.parse_batch(batch_no, "keep-rule@v2")

        lineage_keys = {
            row["lineage_key"]
            for row in store.rows("data_lineage")
            if row.get("来源批次") == batch_no
        }
        assert f"{batch_no}|keep-rule@v1" in lineage_keys
        assert f"{batch_no}|keep-rule@v2" in lineage_keys

        stats = pipeline.get_usage_stats(batch_no)
        assert stats["total"] == 2
        assert stats["total_entities"] == 6

    def test_rerun_same_version_is_idempotent_even_after_switch(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("idem-rule", "dedup-fix-src", version="v1")
        pipeline.publish_parser_rule("idem-rule", "dedup-fix-src", version="v2")
        pipeline.parse_batch(batch_no, "idem-rule@v1")
        pipeline.parse_batch(batch_no, "idem-rule@v2")
        again = pipeline.parse_batch(batch_no, "idem-rule@v1")
        assert again["created"] == 0
        assert again["reused"] == 3
        assert len(_batch_entities(batch_no)) == 6


class TestConcurrentParse:
    """两个操作员并发提交：批次锁 + 回执区分确认结果与运行中尝试。"""

    def test_second_nonblocking_attempt_gets_running_receipt(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("lock-rule", "dedup-fix-src", version="v1")

        entered = threading.Event()
        release = threading.Event()

        def barrier(_event=entered, _release=release):
            _event.set()
            _release.wait(timeout=5)

        pipeline.install_parse_barrier(batch_no, _Barrier(barrier))

        first_receipt: dict = {}
        first_error: list[Exception] = []

        def first_worker():
            try:
                first_receipt.update(
                    pipeline.parse_batch(batch_no, "lock-rule@v1", operator="alice")
                )
            except Exception as exc:  # pragma: no cover - 并发路径失败时留痕
                first_error.append(exc)

        t1 = threading.Thread(target=first_worker)
        t1.start()
        assert entered.wait(timeout=5)

        # 首个解析尚未结束，第二个非阻塞尝试立即拿到运行中回执
        second = pipeline.parse_batch(batch_no, "lock-rule@v1", wait=False, operator="bob")
        status = pipeline.get_parse_status(batch_no)

        release.set()
        t1.join(timeout=5)

        assert not first_error
        assert second["ok"] is False
        assert second["conflict"] is True
        assert second["state"] == "运行中"
        assert second["running_operator"] == "alice"
        assert second["operator"] == "bob"
        assert second["parsed_ids"] == []
        assert first_receipt["state"] == "已确认"
        assert first_receipt["operator"] == "alice"
        assert status["state"] == "运行中"
        assert status["attempt"]["operator"] == "alice"

        # 并发期间只有一套明细进入下游
        assert len(_batch_entities(batch_no, "lock-rule@v1")) == 3

    def test_blocking_attempt_waits_and_gets_confirmed_receipt(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("wait-rule", "dedup-fix-src", version="v1")

        entered = threading.Event()
        release = threading.Event()
        pipeline.install_parse_barrier(
            batch_no, _Barrier(lambda: (entered.set(), release.wait(timeout=5)))
        )

        receipts: list[dict] = []

        def worker(operator):
            receipts.append(
                pipeline.parse_batch(batch_no, "wait-rule@v1", wait=True, operator=operator)
            )

        t1 = threading.Thread(target=worker, args=("alice",))
        t1.start()
        assert entered.wait(timeout=5)
        t2 = threading.Thread(target=worker, args=("bob",))
        t2.start()
        # 给 t2 一点时间阻塞在锁上
        assert not release.is_set()
        release.set()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert len(receipts) == 2
        assert all(r["state"] == "已确认" for r in receipts)
        creators = [r["created"] for r in receipts]
        assert sorted(creators) == [0, 3]
        # 等待者完全复用先到者的明细
        waiter = next(r for r in receipts if r["created"] == 0)
        assert waiter["reused"] == 3
        assert len(_batch_entities(batch_no, "wait-rule@v1")) == 3

    def test_conflict_is_journaled_and_distinguishable(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("conf-rule", "dedup-fix-src", version="v1")

        entered = threading.Event()
        release = threading.Event()
        pipeline.install_parse_barrier(
            batch_no, _Barrier(lambda: (entered.set(), release.wait(timeout=5)))
        )
        before = len(store.rows("parse_journal"))
        t1 = threading.Thread(
            target=lambda: pipeline.parse_batch(batch_no, "conf-rule@v1", operator="alice")
        )
        t1.start()
        assert entered.wait(timeout=5)
        conflict = pipeline.parse_batch(batch_no, "conf-rule@v1", wait=False, operator="bob")
        release.set()
        t1.join(timeout=5)

        assert conflict["state"] == "运行中"
        journals = store.rows("parse_journal")[before:]
        conflict_journal = next(j for j in journals if j["操作类型"] == "解析冲突拦截")
        confirmed = next(j for j in journals if j["操作类型"] == "批量解析")
        assert conflict_journal["attempt_state"] == "运行中"
        assert conflict_journal["running_attempt_id"] == confirmed["attempt_id"]
        assert conflict_journal["operator"] == "bob"
        assert confirmed["attempt_state"] == "已确认"

    def test_parse_status_returns_last_confirmed_receipt_after_finish(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("stat-rule", "dedup-fix-src", version="v1")
        receipt = pipeline.parse_batch(batch_no, "stat-rule@v1", operator="alice")
        status = pipeline.get_parse_status(batch_no)
        assert status["state"] == "已确认"
        assert status["receipt"]["attempt_id"] == receipt["attempt_id"]


class _Barrier:
    """把可调用屏障适配成 Event 接口（wait）。"""

    def __init__(self, fn):
        self._fn = fn

    def wait(self, timeout=None):
        self._fn()


class TestLineageAndUsage:
    """血缘以唯一明细建立用量统计来源，并回写采集来源清单。"""

    def test_single_lineage_per_batch_rule_version(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("lin-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "lin-rule@v1")
        pipeline.parse_batch(batch_no, "lin-rule@v1")
        pipeline.parse_batch(batch_no, "lin-rule@v1")

        rows = [
            r for r in store.rows("data_lineage")
            if r.get("lineage_key") == f"{batch_no}|lin-rule@v1"
        ]
        assert len(rows) == 1
        assert rows[0]["record_count"] == 3
        assert len(json.loads(rows[0]["source_refs"])) == 3

    def test_lineage_written_back_to_batch_and_source(self, pipeline):
        source_id, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("wb-rule", source_id, version="v1")
        receipt = pipeline.parse_batch(batch_no, "wb-rule@v1")

        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == batch_no)
        lineage_ids = json.loads(batch["lineage_ids"])
        assert receipt["lineage_id"] in lineage_ids

        source = next(s for s in store.rows("source_registry") if s["来源标识"] == source_id)
        refs = json.loads(source["collection_refs"])
        assert any(r["lineage_key"] == f"{batch_no}|wb-rule@v1" for r in refs)
        assert all(r["batch_no"] == batch_no for r in refs)

    def test_usage_stats_built_from_unique_lineage(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("use-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "use-rule@v1")
        pipeline.parse_batch(batch_no, "use-rule@v1")

        stats = pipeline.get_usage_stats(batch_no)
        assert stats["ok"] is True
        assert stats["total"] == 1
        assert stats["total_entities"] == 3
        item = stats["items"][0]
        assert item["rule_version_key"] == "use-rule@v1"
        assert item["entity_count"] == 3
        assert len(item["raw_refs"]) == 3

    def test_export_lineage_is_idempotent(self, pipeline, sample_raw_logs):
        _, batch_no = _prepare_batch(pipeline, logs=sample_raw_logs[:2])
        pipeline.publish_parser_rule("exp-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "exp-rule@v1")
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        before = len(store.rows("data_lineage"))
        first = pipeline.build_export(f"view-{batch_no}", "csv")
        second = pipeline.build_export(f"view-{batch_no}", "csv")
        after = len(store.rows("data_lineage"))
        assert after == before + 1  # 仅首次导出建立导出血缘
        assert first["lineage"]["id"] == second["lineage"]["id"]
        consumers = json.loads(second["lineage"]["consumer_refs"])
        assert len(consumers) == 2

    def test_batch_lineage_records_export_consumer(self, pipeline, sample_raw_logs):
        _, batch_no = _prepare_batch(pipeline, logs=sample_raw_logs[:2])
        pipeline.publish_parser_rule("con-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "con-rule@v1")
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        export = pipeline.build_export(f"view-{batch_no}", "csv")

        lineage = next(
            r for r in store.rows("data_lineage")
            if r.get("lineage_key") == f"{batch_no}|con-rule@v1"
        )
        assert export["export"]["id"] and f"export-{export['export']['id']}" in json.loads(
            lineage["consumer_refs"]
        )


class TestTraceability:
    """重复数据残留可从统计明细逐行查原文。"""

    def test_trace_entity_returns_raw_body_and_siblings(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("trace-rule", "dedup-fix-src", version="v1")
        pipeline.publish_parser_rule("trace-rule", "dedup-fix-src", version="v2")
        pipeline.parse_batch(batch_no, "trace-rule@v1")
        pipeline.parse_batch(batch_no, "trace-rule@v2")

        target = _batch_entities(batch_no, "trace-rule@v1")[0]
        result = pipeline.trace_entity(int(target["id"]))
        assert result["ok"] is True
        assert result["raw_record"]["原始正文"] == THREE_LOGS[0]["body"]
        assert result["lineage"]["lineage_key"] == f"{batch_no}|trace-rule@v1"
        sibling_keys = {s["rule_version_key"] for s in result["sibling_entities"]}
        assert "trace-rule@v2" in sibling_keys
        assert any(j["attempt_state"] == "已确认" for j in result["journals"])

    def test_trace_lineage_lists_every_row_with_raw_text(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("tl-rule", "dedup-fix-src", version="v1")
        receipt = pipeline.parse_batch(batch_no, "tl-rule@v1")
        result = pipeline.trace_lineage(receipt["lineage_id"])
        assert result["ok"] is True
        assert len(result["details"]) == 3
        bodies = {d["原始正文"] for d in result["details"]}
        assert bodies == {log["body"] for log in THREE_LOGS}
        assert all(d["natural_key"] for d in result["details"])


class TestDuplicateCleanup:
    """历史重复写入残留的标记清理：保留唯一明细，统计不再重复。"""

    def _inject_legacy_duplicates(self, pipeline, batch_no, rule_key):
        """模拟修复前的重复写入：再手工塞一套同自然键旧格式明细。"""
        parsed = store.rows("parsed_entity")
        raw_rows = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no]
        for raw in raw_rows:
            parsed.append({
                "id": max(int(e["id"]) for e in parsed) + 1,
                "来源批次": batch_no,
                "原始记录ID": str(raw["id"]),
                "解析规则版本": rule_key,
                "标准化字段集": json.dumps({"legacy": True}),
                "校验状态": "",
                "status": "待校验",
                "pending": True,
                "abnormal": False,
                "parser_rule": rule_key.split("@")[0],
                "rule_version_key": rule_key,
            })

    def test_cleanup_marks_residual_and_stats_recover(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("clean-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "clean-rule@v1")
        self._inject_legacy_duplicates(pipeline, batch_no, "clean-rule@v1")
        assert len(_batch_entities(batch_no, "clean-rule@v1")) == 6

        result = pipeline.cleanup_duplicate_entities(batch_no)
        assert result["superseded_entities"] == 3

        active = _batch_entities(batch_no, "clean-rule@v1", include_superseded=False)
        residual = _batch_entities(batch_no, "clean-rule@v1")
        assert len(active) == 3
        assert all(e.get("dup_of") for e in residual if e.get("superseded"))
        # 残留行仍可逐行溯源，没有物理删除
        trace = pipeline.trace_entity(int(active[0]["id"]))
        assert trace["raw_record"] is not None

        stats = pipeline.get_usage_stats(batch_no)
        assert stats["total_entities"] == 3

    def test_cleanup_dedupes_residual_lineage_rows(self, pipeline):
        _, batch_no = _prepare_batch(pipeline)
        pipeline.publish_parser_rule("cl2-rule", "dedup-fix-src", version="v1")
        receipt = pipeline.parse_batch(batch_no, "cl2-rule@v1")
        # 手工复制一条同键血缘，模拟历史残留
        lineages = store.rows("data_lineage")
        origin = next(r for r in lineages if r["id"] == receipt["lineage_id"])
        clone = dict(origin)
        clone["id"] = max(int(r["id"]) for r in lineages) + 1
        lineages.append(clone)

        result = pipeline.cleanup_duplicate_entities(batch_no)
        assert result["superseded_lineages"] == 1
        stats = pipeline.get_usage_stats(batch_no)
        assert stats["total"] == 1


class TestReplayPreservesVersionBasis:
    """回溯重放在新规则版本下产出新明细，旧版本依据不被覆盖。"""

    def test_replay_creates_new_version_keeps_old(self, pipeline, sample_raw_logs):
        _, batch_no = _prepare_batch(pipeline, logs=sample_raw_logs[:2])
        pipeline.publish_parser_rule("replay-rule", "dedup-fix-src", version="v1")
        pipeline.publish_parser_rule("replay-rule", "dedup-fix-src", version="v2")
        pipeline.parse_batch(batch_no, "replay-rule@v1")
        pipeline.validate_entities(batch_no)

        result = pipeline.replay_batch(batch_no, "replay-rule@v2")
        assert result["ok"] is True
        assert result["created_count"] == 2
        assert result["reused_count"] == 0

        v1 = _batch_entities(batch_no, "replay-rule@v1")
        v2 = _batch_entities(batch_no, "replay-rule@v2")
        assert len(v1) == 2 and len(v2) == 2
        # 旧版本明细的字段与状态不被重放改写
        assert all("replay-rule@v1" == e.get("rule_version_key") for e in v1)

    def test_replay_same_version_is_idempotent(self, pipeline, sample_raw_logs):
        _, batch_no = _prepare_batch(pipeline, logs=sample_raw_logs[:2])
        pipeline.publish_parser_rule("ri-rule", "dedup-fix-src", version="v1")
        pipeline.parse_batch(batch_no, "ri-rule@v1")
        first = pipeline.replay_batch(batch_no, "ri-rule@v1")
        second = pipeline.replay_batch(batch_no, "ri-rule@v1")
        assert first["created_count"] == 0 and first["reused_count"] == 2
        assert second["reused_count"] == 2
        assert len(_batch_entities(batch_no, "ri-rule@v1")) == 2
