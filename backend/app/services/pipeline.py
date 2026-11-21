"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。

解析写入约束（修复重复写入后的核心不变量）：
1. 一条标准化明细由「原始记录ID + 解析规则版本」唯一确定（natural_key），
   同一批次重复解析只复用已有明细，不再产生第二套记录；
2. 每个批次同一时刻只允许一个解析尝试进入临界区，并发提交通过回执区分
   「已确认」结果与「运行中」尝试，后者不会向下游写入任何明细；
3. 采集血缘以唯一明细为来源建立，血缘条目按 (批次, 规则版本) 幂等维护，
   并把结果回写到采集批次与数据源来源清单；
4. 规则版本切换后的重放会生成新版本明细，历史版本明细原样保留，
   每个生效过的版本都保留独立依据。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _rule_version_key(rule: dict[str, Any] | None) -> str:
    """规则版本键：显式版本号存在时限定到具体版本，否则退化为规则名。"""
    if not rule:
        return "unknown"
    name = rule.get("规则名称", "unknown")
    version = rule.get("规则版本") or rule.get("version") or ""
    return f"{name}@{version}" if str(version).strip() else str(name)


def _entity_natural_key(raw_id: int | str, rule_version_key: str) -> str:
    """标准化明细自然键：相同原始行 + 相同规则版本 => 唯一明细。"""
    return f"{raw_id}|{rule_version_key}"


def _entity_key_of(entity: dict[str, Any]) -> str | None:
    """取明细自然键；兼容修复前落库、没有 natural_key 字段的旧明细。"""
    if entity.get("natural_key"):
        return str(entity["natural_key"])
    raw_id = entity.get("原始记录ID")
    rule_key = entity.get("rule_version_key") or entity.get("解析规则版本")
    if raw_id and rule_key:
        return _entity_natural_key(raw_id, rule_key)
    return None


def _is_active(entity: dict[str, Any]) -> bool:
    """重复残留（被清理动作标记 superseded）的明细不进入下游消费与统计。"""
    return not entity.get("superseded")


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    def __init__(self) -> None:
        # 批次级解析锁：同一批次的解析临界区全局串行
        self._parse_locks: dict[str, threading.Lock] = {}
        self._parse_lock_guard = threading.Lock()
        # 正在运行的解析尝试：batch_no -> 尝试回执信息
        self._active_parses: dict[str, dict[str, Any]] = {}
        # 最近一次已确认解析回执，供值班员查询尝试结局
        self._last_receipts: dict[str, dict[str, Any]] = {}
        # 测试/演练用屏障：批次进入临界区后先等待该事件，便于确定性复现并发
        self._parse_barriers: dict[str, threading.Event] = {}
        # 测试/演练用临界区延迟（秒），经 HTTP 复现并发提交窗口
        self._parse_delays: dict[str, float] = {}

    # ---------- 并发控制基础设施 ----------
    def _get_parse_lock(self, batch_no: str) -> threading.Lock:
        with self._parse_lock_guard:
            lock = self._parse_locks.get(batch_no)
            if lock is None:
                lock = threading.Lock()
                self._parse_locks[batch_no] = lock
            return lock

    def install_parse_barrier(self, batch_no: str, event: threading.Event) -> None:
        """安装解析屏障（测试/演练用途）：临界区开始后等待事件再继续。"""
        self._parse_barriers[batch_no] = event

    def set_parse_delay(self, batch_no: str, seconds: float) -> None:
        """设置解析临界区延迟（测试/演练用途），用于经 HTTP 复现并发窗口。"""
        self._parse_delays[batch_no] = max(0.0, float(seconds))

    def inject_legacy_duplicates(self, batch_no: str, rule_version_key: str) -> dict[str, Any]:
        """测试/演练用途：模拟修复前的重复写入，为批次再塞一套同键旧格式明细。"""
        parsed = store.rows("parsed_entity")
        raw_rows = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no]
        if not raw_rows:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}
        rule_name = rule_version_key.split("@", 1)[0]
        injected = 0
        for raw in raw_rows:
            next_id = max((int(e["id"]) for e in parsed), default=0) + 1
            parsed.append({
                "id": next_id,
                "来源批次": batch_no,
                "原始记录ID": str(raw["id"]),
                "解析规则版本": rule_version_key,
                "标准化字段集": json.dumps({"legacy": True}),
                "校验状态": "",
                "status": "待校验",
                "pending": True,
                "abnormal": False,
                "parser_rule": rule_name,
                "rule_version_key": rule_version_key,
            })
            injected += 1
        return {"ok": True, "injected": injected}

    def get_parse_status(self, batch_no: str) -> dict[str, Any]:
        """查询批次解析尝试状态：运行中的尝试或最近一次已确认回执。"""
        active = self._active_parses.get(batch_no)
        if active is not None:
            return {
                "ok": True,
                "state": "运行中",
                "batch_no": batch_no,
                "attempt": active,
            }
        receipt = self._last_receipts.get(batch_no)
        if receipt is not None:
            return {"ok": True, "state": "已确认", "batch_no": batch_no, "receipt": receipt}
        return {"ok": True, "state": "无尝试", "batch_no": batch_no}

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
            "collection_refs": "[]",
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    def publish_parser_rule(
        self,
        rule_name: str,
        data_source: str,
        input_format: str = "pipe",
        output_fields: list[str] | None = None,
        engine: str = "regex",
        version: str = "",
    ) -> dict[str, Any]:
        """登记/发布一条带显式版本号的字段解析规则，供解析与重放选用。"""
        parsers = store.rows("field_parser")
        rule_version_key = _rule_version_key(
            {"规则名称": rule_name, "规则版本": version}
        )
        for rule in parsers:
            if _rule_version_key(rule) == rule_version_key:
                rule["status"] = "已发布"
                rule["pending"] = False
                return {"ok": True, "message": f"解析规则 {rule_version_key} 已存在并处于发布态", "rule": rule}
        entry = {
            "id": _next_id("field_parser"),
            "规则名称": rule_name,
            "适用数据源": data_source,
            "输入格式": input_format,
            "输出字段列表": ",".join(output_fields or ["time", "level", "message"]),
            "解析引擎": engine,
            "规则版本": version,
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
        }
        parsers.append(entry)
        return {"ok": True, "message": f"解析规则 {rule_version_key} 已发布", "rule": entry}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
            "parsed_ids": "[]",
            "lineage_ids": "[]",
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                    # 原始行在批次内的稳定行键，供重复解析沿引用核查同一行
                    "row_key": f"{batch_no}#L{idx + 1}",
                    "parsed_refs": "[]",
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析（幂等 upsert + 批次锁） ----------
    def _resolve_rule(self, parser_rule_name: str | None) -> dict[str, Any] | None:
        parsers = store.rows("field_parser")
        if parser_rule_name:
            # 显式指定「规则名@版本」时优先精确匹配到具体版本
            if "@" in parser_rule_name:
                for p in parsers:
                    if _rule_version_key(p) == parser_rule_name:
                        return p
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    return p
            # 允许重放指定一个尚未登记的候选规则：构造合成规则描述符
            return {"规则名称": parser_rule_name, "规则版本": "", "输出字段列表": ""}
        for p in parsers:
            if p.get("status") == "已发布":
                return p
        return None

    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
        *,
        wait: bool = True,
        operator: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。

        - 同 (原始行, 规则版本) 的明细天然唯一，重复提交只复用不新增；
        - ``wait=False`` 时若批次已有解析在运行，立即返回「运行中」冲突回执，
          本次尝试不写入任何下游数据；``wait=True`` 时等待首个尝试结束，
          再以幂等方式拿到同一份「已确认」明细回执。
        """
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        rule = self._resolve_rule(parser_rule_name)
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}
        rule_version_key = _rule_version_key(rule)

        attempt_id = f"ATT-{uuid.uuid4().hex[:8]}"
        operator = operator or "anonymous"
        lock = self._get_parse_lock(batch_no)
        acquired = lock.acquire(blocking=wait)
        if not acquired:
            # 并发冲突：首个解析尚未结束，本次尝试不进入下游消费
            running = dict(self._active_parses.get(batch_no) or {})
            receipt = {
                "ok": False,
                "conflict": True,
                "state": "运行中",
                "message": (
                    f"批次 {batch_no} 已有解析尝试 {running.get('attempt_id')} 运行中，"
                    f"本次尝试 {attempt_id} 未进入下游"
                ),
                "attempt_id": attempt_id,
                "operator": operator,
                "running_attempt_id": running.get("attempt_id"),
                "running_operator": running.get("operator"),
                "rule_version_key": running.get("rule_version_key", rule_version_key),
                "success": 0,
                "failure": 0,
                "created": 0,
                "reused": 0,
                "parsed_ids": [],
                "failed_ids": [],
            }
            self._journal(
                op_type="解析冲突拦截",
                batch_no=batch_no,
                rule_version_key=rule_version_key,
                attempt_id=attempt_id,
                operator=operator,
                state="运行中",
                extra={"running_attempt_id": running.get("attempt_id")},
            )
            return receipt

        try:
            self._active_parses[batch_no] = {
                "attempt_id": attempt_id,
                "operator": operator,
                "rule_version_key": rule_version_key,
                "started_at": _now_iso(),
                "state": "运行中",
            }
            result = self._parse_batch_locked(
                batch_no=batch_no,
                rule=rule,
                attempt_id=attempt_id,
                operator=operator,
                source="批量解析",
            )
        finally:
            self._active_parses.pop(batch_no, None)
            lock.release()

        self._last_receipts[batch_no] = result
        return result

    def _parse_batch_locked(
        self,
        batch_no: str,
        rule: dict[str, Any],
        *,
        attempt_id: str,
        operator: str,
        source: str,
        replay_id: str | None = None,
    ) -> dict[str, Any]:
        """解析临界区内部实现：按自然键幂等写入标准化明细。"""
        # 确定性并发测试屏障：拿到锁之后、写入之前挂起
        barrier = self._parse_barriers.pop(batch_no, None)
        if barrier is not None:
            barrier.wait(timeout=10)
        delay = self._parse_delays.get(batch_no, 0.0)
        if delay:
            time.sleep(delay)

        rule_version_key = _rule_version_key(rule)
        lineage_key = f"{batch_no}|{rule_version_key}"

        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break
        schema_version = schema_ver.get("版本号") if schema_ver else "v1.0"

        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        parsed_entities = store.rows("parsed_entity")

        # 该批次 + 该规则版本下已有明细索引（含修复前无 natural_key 的旧明细）
        existing_index: dict[str, dict[str, Any]] = {}
        for ent in parsed_entities:
            if ent.get("来源批次") != batch_no:
                continue
            if ent.get("rule_version_key", ent.get("解析规则版本")) != rule_version_key:
                continue
            key = _entity_key_of(ent)
            if key:
                existing_index[key] = ent

        success_count = 0
        failure_count = 0
        created_count = 0
        reused_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []
        touched_raw_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                natural_key = _entity_natural_key(raw_id, rule_version_key)
                existing = existing_index.get(natural_key)
                if existing is not None:
                    # 重复解析：复用唯一明细，仅刷新解析产物，保留既有校验状态与身份
                    existing["标准化字段集"] = json.dumps(fields, ensure_ascii=False)
                    existing["content_digest"] = _stable_digest(fields)
                    existing["reparsed_at"] = _now_iso()
                    existing["replay_id"] = replay_id or existing.get("replay_id")
                    ent_id = int(existing["id"])
                    reused_count += 1
                else:
                    ent_id = _next_id("parsed_entity")
                    entity = {
                        "id": ent_id,
                        "来源批次": batch_no,
                        "原始记录ID": str(raw_id),
                        "解析规则版本": rule_version_key,
                        "标准化字段集": json.dumps(fields, ensure_ascii=False),
                        "校验状态": "",
                        "status": "待校验",
                        "pending": True,
                        "abnormal": False,
                        "parser_rule": rule.get("规则名称"),
                        "rule_version_key": rule_version_key,
                        "schema_version": schema_version,
                        "parsed_at": _now_iso(),
                        "content_digest": _stable_digest(fields),
                        "natural_key": natural_key,
                        "raw_ref": str(raw_id),
                        "row_key": raw.get("row_key", f"{batch_no}#L{raw.get('原始行号')}"),
                        "lineage_key": lineage_key,
                        "replay_id": replay_id,
                    }
                    parsed_entities.append(entity)
                    created_count += 1
                self._link_raw_to_entity(raw, ent_id, rule_version_key)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
                touched_raw_ids.append(raw_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 批次清单回写：维护该批次全部唯一明细（跨规则版本），不再被重复提交覆盖
        self._sync_batch_refs(batch_no)

        # 失败记录幂等进入重试队列：同 (原始行, 批次, 规则版本) 只挂一次
        if failure_count > 0:
            self._enqueue_retries(batch_no, rule_version_key, failed_ids)

        # 以唯一明细建立/刷新批次血缘，并回写采集来源清单
        lineage = self._sync_batch_lineage(
            batch_no=batch_no,
            rule_version_key=rule_version_key,
            rule=rule,
            schema_version=schema_version,
            replay_id=replay_id,
        )

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        self._journal(
            op_type=source,
            batch_no=batch_no,
            rule_version_key=rule_version_key,
            attempt_id=attempt_id,
            operator=operator,
            state="已确认",
            success_count=success_count,
            failure_count=failure_count,
            extra={
                "created_count": created_count,
                "reused_count": reused_count,
                "lineage_key": lineage_key,
                "replay_id": replay_id,
            },
        )

        return {
            "ok": True,
            "state": "已确认",
            "message": f"批次 {batch_no} 解析完成",
            "attempt_id": attempt_id,
            "operator": operator,
            "batch_no": batch_no,
            "rule_version_key": rule_version_key,
            "schema_version": schema_version,
            "lineage_key": lineage_key,
            "lineage_id": lineage["id"],
            "success": success_count,
            "failure": failure_count,
            "created": created_count,
            "reused": reused_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
            "raw_refs": touched_raw_ids,
        }

    def _link_raw_to_entity(
        self, raw: dict[str, Any], entity_id: int, rule_version_key: str
    ) -> None:
        """原始行引用去重回写：重复解析不会让同一行多出一套引用。"""
        try:
            refs = json.loads(raw.get("parsed_refs") or "[]")
        except (json.JSONDecodeError, TypeError):
            refs = []
        if not any(
            str(ref.get("entity_id")) == str(entity_id)
            and ref.get("rule_version_key") == rule_version_key
            for ref in refs
        ):
            refs.append({"entity_id": entity_id, "rule_version_key": rule_version_key})
        raw["parsed_refs"] = json.dumps(refs, ensure_ascii=False)

    def _sync_batch_refs(self, batch_no: str) -> None:
        """把批次下全部唯一明细与血缘回写到采集批次清单。"""
        entity_ids = sorted(
            int(ent["id"])
            for ent in store.rows("parsed_entity")
            if ent.get("来源批次") == batch_no and _is_active(ent)
        )
        lineage_ids = sorted(
            int(row["id"])
            for row in store.rows("data_lineage")
            if row.get("来源批次") == batch_no and row.get("lineage_key") and _is_active(row)
        )
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                b["parsed_ids"] = json.dumps(entity_ids)
                b["lineage_ids"] = json.dumps(lineage_ids)
                break

    def _enqueue_retries(
        self, batch_no: str, rule_version_key: str, failed_ids: list[int]
    ) -> None:
        retry_q = store.rows("retry_queue")
        existing = {
            (str(r.get("失败记录ID")), r.get("batch_no"), r.get("rule_version_key"))
            for r in retry_q
        }
        for fid in failed_ids:
            dedup_key = (str(fid), batch_no, rule_version_key)
            if dedup_key in existing:
                continue
            retry_entry = {
                "id": _next_id("retry_queue"),
                "失败记录ID": str(fid),
                "失败原因": "字段解析失败",
                "重试次数": "0",
                "最大重试": "3",
                "下次重试时间": _now_iso(),
                "status": "待重试",
                "pending": True,
                "abnormal": False,
                "batch_no": batch_no,
                "parser_rule": rule_version_key,
                "rule_version_key": rule_version_key,
            }
            retry_q.append(retry_entry)
            existing.add(dedup_key)

    def _sync_batch_lineage(
        self,
        batch_no: str,
        rule_version_key: str,
        rule: dict[str, Any],
        schema_version: str,
        replay_id: str | None,
    ) -> dict[str, Any]:
        """以唯一解析明细建立用量统计来源的血缘条目（幂等 upsert）。"""
        lineage_key = f"{batch_no}|{rule_version_key}"
        entities = [
            ent
            for ent in store.rows("parsed_entity")
            if ent.get("来源批次") == batch_no
            and ent.get("rule_version_key", ent.get("解析规则版本")) == rule_version_key
            and _is_active(ent)
        ]
        entity_ids = sorted(int(ent["id"]) for ent in entities)
        raw_refs = sorted({int(ent["原始记录ID"]) for ent in entities})

        lineages = store.rows("data_lineage")
        existing = None
        for row in lineages:
            if row.get("lineage_key") == lineage_key:
                existing = row
                break

        if existing is not None:
            existing["标准化版本"] = schema_version
            existing["entity_ids"] = json.dumps(entity_ids)
            existing["source_refs"] = json.dumps(raw_refs)
            existing["record_count"] = len(entity_ids)
            existing["记录时间"] = _now_iso()
            existing["replay_id"] = replay_id or existing.get("replay_id")
            lineage_row = existing
        else:
            lineage_row = {
                "id": _next_id("data_lineage"),
                "来源批次": batch_no,
                "解析规则版本": rule_version_key,
                "标准化版本": schema_version,
                "下游消费标识": f"parsed-usage:{batch_no}:{rule_version_key}",
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "lineage_key": lineage_key,
                "batch_no": batch_no,
                "rule_version_key": rule_version_key,
                "entity_ids": json.dumps(entity_ids),
                "source_refs": json.dumps(raw_refs),
                "consumer_refs": "[]",
                "record_count": len(entity_ids),
                "replay_id": replay_id,
            }
            lineages.append(lineage_row)

        self._sync_batch_refs(batch_no)
        self._writeback_source_refs(batch_no, lineage_row)
        return lineage_row

    def _writeback_source_refs(self, batch_no: str, lineage_row: dict[str, Any]) -> None:
        """血缘结果回写采集来源清单（数据源登记），按血缘键去重。"""
        source_id = None
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                source_id = b.get("所属数据源")
                break
        if source_id is None:
            return
        ref_item = {
            "batch_no": batch_no,
            "lineage_id": lineage_row["id"],
            "lineage_key": lineage_row["lineage_key"],
            "rule_version_key": lineage_row["解析规则版本"],
            "record_count": lineage_row.get("record_count", 0),
        }
        for source in store.rows("source_registry"):
            if source.get("来源标识") != source_id:
                continue
            try:
                refs = json.loads(source.get("collection_refs") or "[]")
            except (json.JSONDecodeError, TypeError):
                refs = []
            refs = [r for r in refs if r.get("lineage_key") != ref_item["lineage_key"]]
            refs.append(ref_item)
            source["collection_refs"] = json.dumps(refs, ensure_ascii=False)
            break

    def _journal(
        self,
        *,
        op_type: str,
        batch_no: str,
        rule_version_key: str,
        attempt_id: str,
        operator: str,
        state: str,
        success_count: int = 0,
        failure_count: int = 0,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """记录解析操作日志；冲突拦截与已确认解析可从字段区分。"""
        extra = extra or {}
        if state == "运行中":
            journal_status = "失败"
            result_text = "冲突"
            pending = True
            abnormal = True
        else:
            journal_status = "成功" if failure_count == 0 else "部分成功"
            result_text = journal_status
            pending = failure_count > 0
            abnormal = failure_count > 0
        entry = {
            "id": _next_id("parse_journal"),
            "操作类型": op_type,
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": result_text,
            "status": journal_status,
            "pending": pending,
            "abnormal": abnormal,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
            "attempt_id": attempt_id,
            "operator": operator,
            "attempt_state": state,
            "rule_version_key": rule_version_key,
            "created_count": str(extra.get("created_count", 0)),
            "reused_count": str(extra.get("reused_count", 0)),
            "lineage_key": extra.get("lineage_key", ""),
            "replay_id": extra.get("replay_id"),
            "running_attempt_id": extra.get("running_attempt_id"),
        }
        store.rows("parse_journal").append(entry)
        return entry

    # ---------- 用量统计与逐行溯源 ----------
    def get_usage_stats(self, batch_no: str | None = None) -> dict[str, Any]:
        """基于唯一血缘汇总明细用量：每条血缘对应一套唯一解析明细。"""
        items: list[dict[str, Any]] = []
        for row in store.rows("data_lineage"):
            if not row.get("lineage_key") or not _is_active(row):
                continue
            if batch_no and row.get("来源批次") != batch_no:
                continue
            entity_ids = json.loads(row.get("entity_ids") or "[]")
            items.append({
                "lineage_id": row["id"],
                "lineage_key": row["lineage_key"],
                "batch_no": row["来源批次"],
                "rule_version_key": row["解析规则版本"],
                "schema_version": row["标准化版本"],
                "entity_count": row.get("record_count", len(entity_ids)),
                "raw_refs": json.loads(row.get("source_refs") or "[]"),
                "downstream_consumers": json.loads(row.get("consumer_refs") or "[]"),
            })
        return {
            "ok": True,
            "items": items,
            "total": len(items),
            "total_entities": sum(int(i["entity_count"]) for i in items),
        }

    def trace_entity(self, entity_id: int) -> dict[str, Any]:
        """从标准化明细逐行回查原始正文、血缘与同原始行的其他规则版本明细。"""
        entity = store.find("parsed_entity", entity_id)
        if entity is None:
            return {"ok": False, "message": f"标准化明细 {entity_id} 不存在"}
        raw = None
        raw_id_text = entity.get("原始记录ID")
        try:
            raw = store.find("raw_records", int(raw_id_text))
        except (TypeError, ValueError):
            raw = None

        lineage = None
        for row in store.rows("data_lineage"):
            ids = json.loads(row.get("entity_ids") or "[]")
            if entity_id in ids:
                lineage = row
                break

        siblings = [
            {
                "id": ent["id"],
                "rule_version_key": ent.get("rule_version_key", ent.get("解析规则版本")),
                "natural_key": _entity_key_of(ent),
                "status": ent.get("status"),
            }
            for ent in store.rows("parsed_entity")
            if ent.get("原始记录ID") == raw_id_text and int(ent["id"]) != entity_id
        ]
        journals = [
            {
                "id": j["id"],
                "操作类型": j.get("操作类型"),
                "执行结果": j.get("执行结果"),
                "attempt_state": j.get("attempt_state"),
                "attempt_id": j.get("attempt_id"),
                "operator": j.get("operator"),
                "执行时间": j.get("执行时间"),
            }
            for j in store.rows("parse_journal")
            if j.get("批次编号") == entity.get("来源批次")
        ]
        return {
            "ok": True,
            "entity": entity,
            "raw_record": raw,
            "lineage": lineage,
            "sibling_entities": siblings,
            "journals": journals,
        }

    def trace_lineage(self, lineage_id: int) -> dict[str, Any]:
        """从血缘条目逐行展开明细及其原始正文，用于核查重复数据残留。"""
        lineage_row = store.find("data_lineage", lineage_id)
        if lineage_row is None:
            return {"ok": False, "message": f"血缘条目 {lineage_id} 不存在"}
        entity_ids = json.loads(lineage_row.get("entity_ids") or "[]")
        details = []
        for ent_id in entity_ids:
            ent = store.find("parsed_entity", int(ent_id))
            if ent is None:
                continue
            raw = store.find("raw_records", int(ent["原始记录ID"]))
            details.append({
                "entity_id": ent["id"],
                "natural_key": _entity_key_of(ent),
                "raw_id": ent["原始记录ID"],
                "row_key": ent.get("row_key"),
                "原始正文": raw.get("原始正文") if raw else None,
                "标准化字段集": ent.get("标准化字段集"),
                "status": ent.get("status"),
            })
        return {"ok": True, "lineage": lineage_row, "details": details}

    def cleanup_duplicate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """清理历史重复写入残留：同自然键保留最早一条，其余标记为重复残留。

        残留行不会被物理删除，仍可通过溯源接口逐行查原文，但不再进入
        血缘、用量统计、聚合与下游消费。
        """
        groups: dict[str, list[dict[str, Any]]] = {}
        for ent in store.rows("parsed_entity"):
            if not _is_active(ent):
                continue
            if batch_no and ent.get("来源批次") != batch_no:
                continue
            key = _entity_key_of(ent)
            if not key:
                continue
            groups.setdefault(key, []).append(ent)

        superseded_entities = 0
        for group in groups.values():
            if len(group) <= 1:
                continue
            group.sort(key=lambda e: int(e["id"]))
            keep = group[0]
            for dup in group[1:]:
                dup["superseded"] = True
                dup["dup_of"] = keep["id"]
                dup["清理状态"] = "重复残留"
                dup["abnormal"] = True
                superseded_entities += 1

        # 血缘条目按 lineage_key 做同样的去重残留标记
        lineage_groups: dict[str, list[dict[str, Any]]] = {}
        for row in store.rows("data_lineage"):
            key = row.get("lineage_key")
            if not key or not _is_active(row):
                continue
            if batch_no and row.get("来源批次") != batch_no:
                continue
            lineage_groups.setdefault(key, []).append(row)
        superseded_lineages = 0
        for group in lineage_groups.values():
            if len(group) <= 1:
                continue
            group.sort(key=lambda e: int(e["id"]))
            for dup in group[1:]:
                dup["superseded"] = True
                dup["清理状态"] = "重复残留"
                dup["abnormal"] = True
                superseded_lineages += 1

        self._rebuild_active_lineage_refs()
        return {
            "ok": True,
            "message": "重复残留清理完成",
            "superseded_entities": superseded_entities,
            "superseded_lineages": superseded_lineages,
        }

    def _rebuild_active_lineage_refs(self) -> None:
        """按当前有效明细重建血缘引用、批次清单与数据源来源清单。"""
        for row in store.rows("data_lineage"):
            if not row.get("lineage_key") or not _is_active(row):
                continue
            entities = [
                ent
                for ent in store.rows("parsed_entity")
                if ent.get("来源批次") == row.get("来源批次")
                and ent.get("rule_version_key", ent.get("解析规则版本")) == row.get("解析规则版本")
                and _is_active(ent)
            ]
            entity_ids = sorted(int(ent["id"]) for ent in entities)
            raw_refs = sorted({int(ent["原始记录ID"]) for ent in entities})
            row["entity_ids"] = json.dumps(entity_ids)
            row["source_refs"] = json.dumps(raw_refs)
            row["record_count"] = len(entity_ids)
            self._sync_batch_refs(row["来源批次"])
            self._writeback_source_refs(row["来源批次"], row)

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重（重复残留明细不参与下游去重）
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过" or not _is_active(rec):
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验" and _is_active(p)]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图（重复残留明细不计入统计）。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过" or not _is_active(entity):
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len([e for e in parsed if _is_active(e)]),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘：同一视图同一导出格式幂等复用，重复导出不再新增血缘行
        export_lineage_key = f"export|{view_name}|{export_format}"
        consumer_id = f"export-{export_id}"
        lineage_entry = None
        for row in lineages:
            if row.get("lineage_key") == export_lineage_key:
                lineage_entry = row
                break
        if lineage_entry is not None:
            try:
                consumers = json.loads(lineage_entry.get("consumer_refs") or "[]")
            except (json.JSONDecodeError, TypeError):
                consumers = []
            if consumer_id not in consumers:
                consumers.append(consumer_id)
            lineage_entry["consumer_refs"] = json.dumps(consumers, ensure_ascii=False)
            lineage_entry["下游消费标识"] = consumer_id
            lineage_entry["记录时间"] = _now_iso()
        else:
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": view_name,
                "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
                "标准化版本": view_name,
                "下游消费标识": consumer_id,
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "export_id": str(export_id),
                "export_format": export_format,
                "lineage_key": export_lineage_key,
                "consumer_refs": json.dumps([consumer_id], ensure_ascii=False),
            }
            lineages.append(lineage_entry)

        # 关联回批次解析血缘：view-<批次编号> 命名约定下挂接来源
        if view_name.startswith("view-"):
            source_batch = view_name[len("view-"):]
            for row in lineages:
                if row.get("来源批次") == source_batch and row.get("lineage_key", "").startswith(source_batch):
                    try:
                        consumers = json.loads(row.get("consumer_refs") or "[]")
                    except (json.JSONDecodeError, TypeError):
                        consumers = []
                    if consumer_id not in consumers:
                        consumers.append(consumer_id)
                    row["consumer_refs"] = json.dumps(consumers, ensure_ascii=False)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。

        新版本明细按新自然键写入，旧规则版本产出的明细原样保留，
        因此过去生效的每个版本都保留独立依据；同一版本重复重放幂等。
        """
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        batch_nos = [part.strip() for part in history_batch_range.split(",") if part.strip()]
        rule = self._resolve_rule(new_parser_rule)
        if rule is None:
            rule = {"规则名称": new_parser_rule, "规则版本": "", "输出字段列表": ""}
        rule_version_key = _rule_version_key(rule)

        created_total = 0
        reused_total = 0
        failure_total = 0
        touched_batches: list[str] = []
        for batch_no in batch_nos:
            batch_raw = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no]
            if not batch_raw:
                continue
            lock = self._get_parse_lock(batch_no)
            with lock:
                self._active_parses[batch_no] = {
                    "attempt_id": f"REPLAY-{replay_id}",
                    "operator": "replay-engine",
                    "rule_version_key": rule_version_key,
                    "started_at": _now_iso(),
                    "state": "运行中",
                }
                try:
                    result = self._parse_batch_locked(
                        batch_no=batch_no,
                        rule=rule,
                        attempt_id=f"REPLAY-{replay_id}",
                        operator="replay-engine",
                        source="回溯重放",
                        replay_id=str(replay_id),
                    )
                finally:
                    self._active_parses.pop(batch_no, None)
            self._last_receipts[batch_no] = result
            created_total += result["created"]
            reused_total += result["reused"]
            failure_total += result["failure"]
            touched_batches.append(batch_no)

        replay_entry["产出记录数"] = str(created_total)
        replay_entry["created_count"] = created_total
        replay_entry["reused_count"] = reused_total
        replay_entry["failure_count"] = failure_total
        replay_entry["touched_batches"] = json.dumps(touched_batches, ensure_ascii=False)
        replay_entry["rule_version_key"] = rule_version_key
        replay_entry["status"] = "部分失败" if failure_total else "已完成"
        replay_entry["重放状态"] = replay_entry["status"]
        replay_entry["pending"] = failure_total > 0
        replay_entry["abnormal"] = failure_total > 0

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": created_total + reused_total,
            "created_count": created_total,
            "reused_count": reused_total,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()
