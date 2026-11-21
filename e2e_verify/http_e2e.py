"""端到端 HTTP 验收脚本：用 stdlib urllib 打真实 uvicorn 服务。

场景：同规则并行解析两次三行日志 → 切换候选规则重跑 →
核对解析回执的版本、明细用量与血缘关联，并覆盖并发冲突回执、
用量统计、逐行溯源与重复残留清理接口。
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8013"
THREE_LOGS = [
    {"body": "2026-01-15T10:30:00|INFO|server started"},
    {"body": "2026-01-15T10:31:00|WARN|memory high"},
    {"body": "2026-01-15T10:32:00|ERROR|connection timeout"},
]


def call(method: str, path: str, payload: dict | None = None, query: str = ""):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path + query, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def expect(cond: bool, label: str) -> None:
    if not cond:
        raise AssertionError(f"断言失败: {label}")
    print(f"  ✓ {label}")


def main() -> None:
    status, health = call("GET", "/api/health")
    expect(status == 200 and health["ok"], "健康检查通过")

    source_id = f"e2e-src-{int(time.time())}"
    call("POST", "/api/pipeline/source/register",
         {"source_id": source_id, "protocol": "http", "region": "default"})
    call("POST", "/api/pipeline/source/validate", query=f"?source_id={source_id}")

    status, batch = call("POST", "/api/pipeline/batch/start", {
        "source_id": source_id, "time_window": "2026-01-15", "raw_logs": THREE_LOGS,
    })
    batch_no = batch["batch"]["批次编号"]
    expect(batch["record_count"] == 3, f"批次 {batch_no} 采集 3 行原始日志")

    call("POST", "/api/pipeline/rule/publish", {
        "rule_name": "e2e-rule", "data_source": source_id, "version": "v1",
        "output_fields": ["time", "level", "message"],
    })
    call("POST", "/api/pipeline/rule/publish", {
        "rule_name": "e2e-rule", "data_source": source_id, "version": "v2",
        "output_fields": ["ts", "lvl", "msg"],
    })

    # 1) 同规则连续解析两次
    _, r1 = call("POST", "/api/pipeline/batch/parse",
                 {"batch_no": batch_no, "parser_rule_name": "e2e-rule@v1", "operator": "alice"})
    _, r2 = call("POST", "/api/pipeline/batch/parse",
                 {"batch_no": batch_no, "parser_rule_name": "e2e-rule@v1", "operator": "bob"})
    expect(r1["state"] == "已确认" and r1["created"] == 3, "首次解析已确认并新增 3 条明细")
    expect(r2["reused"] == 3 and r2["created"] == 0, "第二次解析全部复用，无重复写入")
    expect(r1["rule_version_key"] == "e2e-rule@v1", "回执携带规则版本")
    expect(len(r1["parsed_ids"]) == 3 and len(r1["raw_refs"]) == 3, "回执明细用量为 3 行")
    expect(r1["lineage_id"], "回执携带血缘关联 ID")

    # 2) 并发冲突：非阻塞尝试拿到运行中回执
    call("POST", "/api/pipeline/test/parse-delay",
         {"batch_no": batch_no, "seconds": 2})
    holder: dict = {}

    def worker():
        holder.update(call("POST", "/api/pipeline/batch/parse", {
            "batch_no": batch_no, "parser_rule_name": "e2e-rule@v1",
            "wait": True, "operator": "carol",
        })[1])

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.5)  # 等待 worker 进入解析临界区
    _, conflict = call("POST", "/api/pipeline/batch/parse", {
        "batch_no": batch_no, "parser_rule_name": "e2e-rule@v1",
        "wait": False, "operator": "dave",
    })
    _, running_status = call("GET", "/api/pipeline/batch/parse-status",
                             query=f"?batch_no={batch_no}")
    t.join(timeout=10)

    expect(conflict["state"] == "运行中" and conflict["conflict"] is True,
           "并发时第二尝试收到运行中冲突回执")
    expect(conflict["running_operator"] == "carol" and conflict["operator"] == "dave",
           "回执区分在跑尝试与本次尝试操作员")
    expect(running_status["state"] == "运行中", "状态查询显示运行中尝试")
    expect(holder["state"] == "已确认", "持锁尝试最终已确认")
    call("POST", "/api/pipeline/test/parse-delay", {"batch_no": batch_no, "seconds": 0})

    # 3) 切换候选规则版本重跑
    _, v2 = call("POST", "/api/pipeline/batch/parse",
                 {"batch_no": batch_no, "parser_rule_name": "e2e-rule@v2", "operator": "alice"})
    expect(v2["created"] == 3 and v2["rule_version_key"] == "e2e-rule@v2",
           "候选规则 v2 独立产出 3 条新版本明细")

    # 4) 用量统计与血缘
    _, stats = call("GET", "/api/pipeline/usage/stats", query=f"?batch_no={batch_no}")
    expect(stats["total"] == 2 and stats["total_entities"] == 6,
           "用量统计按两套唯一血缘汇总共 6 条明细")

    lineage_id = r1["lineage_id"]
    _, trace = call("GET", f"/api/pipeline/lineage/{lineage_id}/trace")
    expect(len(trace["details"]) == 3, "血缘逐行展开 3 条明细")
    expect({d["原始正文"] for d in trace["details"]} == {log["body"] for log in THREE_LOGS},
           "逐行可回查原始正文")

    entity_id = r1["parsed_ids"][0]
    _, entity_trace = call("GET", f"/api/pipeline/entity/{entity_id}/trace")
    sibling_versions = {s["rule_version_key"] for s in entity_trace["sibling_entities"]}
    expect("e2e-rule@v2" in sibling_versions, "同一原文可沿引用核查另一规则版本明细")

    # 5) 重复残留清理（注入修复前的重复行后清理）
    _, inject = call("POST", "/api/pipeline/test/inject-duplicates",
                     {"batch_no": batch_no, "rule_version_key": "e2e-rule@v1"})
    expect(inject["injected"] == 3, "测试端点注入 3 条修复前形态重复明细")
    _, cleanup = call("POST", "/api/pipeline/cleanup/duplicates",
                      query=f"?batch_no={batch_no}")
    expect(cleanup["superseded_entities"] == 3, "清理接口标记 3 条重复残留")
    _, stats_after = call("GET", "/api/pipeline/usage/stats", query=f"?batch_no={batch_no}")
    expect(stats_after["total_entities"] == 6, "清理后用量统计回到唯一明细口径")

    # 6) 资源列表接口可按批次过滤，残留仍可查
    _, page = call("GET", "/api/parsed_entity",
                   query=f"?{urllib.parse.urlencode({'来源批次': batch_no, 'size': 100})}")
    expect(page["total"] == 9, "标准化明细列表仍保留全部行（含 3 条残留可溯源）")

    print("\n全部 HTTP 端到端验收通过")


if __name__ == "__main__":
    sys.path.insert(0, "/workspace/backend")
    main()
