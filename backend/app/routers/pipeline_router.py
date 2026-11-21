"""流水线路由：暴露跨模块数据采集流水线的 HTTP 接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.pipeline import pipeline

router = APIRouter(prefix="/api/pipeline", tags=["采集流水线"])


class SourceRegisterPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    protocol: str = "http"
    region: str = "default"


class IngestBatchPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)


class ParseBatchPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    parser_rule_name: str | None = None
    # False 时若批次已有解析在运行，立即返回运行中冲突回执，不进入下游
    wait: bool = True
    operator: str | None = None


class PublishRulePayload(BaseModel):
    rule_name: str = Field(..., min_length=1)
    data_source: str = Field(..., min_length=1)
    input_format: str = "pipe"
    output_fields: list[str] = Field(default_factory=lambda: ["time", "level", "message"])
    engine: str = "regex"
    version: str = ""


class ParseDelayPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    seconds: float = Field(..., ge=0)


class InjectDuplicatePayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    rule_version_key: str = Field(..., min_length=1)


class DedupPayload(BaseModel):
    dedup_key_fields: list[str] = Field(..., min_length=1)
    window: str = "batch"
    retain_strategy: str = "keep_first"


class AggregationPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    dim_field: str = Field(..., min_length=1)
    metric_field: str = "count"
    agg_func: str = "count"


class ExportPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    export_format: str = "csv"
    filters: dict[str, str] | None = None


class WatermarkPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str = Field(..., min_length=1)
    watermark_value: str = Field(..., min_length=1)


class ReplayPayload(BaseModel):
    history_batch_range: str = Field(..., min_length=1)
    new_parser_rule: str = Field(..., min_length=1)


class SchemaEvolvePayload(BaseModel):
    version: str = Field(..., min_length=1)
    data_source: str = Field(..., min_length=1)
    field_defs: list[str] = Field(..., min_length=1)
    backward_compatible: bool = True


class FullPipelinePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)
    parser_rule: str | None = None


@router.post("/source/register")
def pipeline_register_source(payload: SourceRegisterPayload) -> dict[str, Any]:
    return pipeline.register_source(payload.source_id, payload.protocol, payload.region)


@router.post("/source/validate")
def pipeline_validate_source(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.validate_source(source_id)


@router.post("/batch/start")
def pipeline_start_batch(payload: IngestBatchPayload) -> dict[str, Any]:
    return pipeline.start_ingest_batch(payload.source_id, payload.time_window, payload.raw_logs)


@router.post("/rule/publish")
def pipeline_publish_rule(payload: PublishRulePayload) -> dict[str, Any]:
    return pipeline.publish_parser_rule(
        payload.rule_name,
        payload.data_source,
        payload.input_format,
        payload.output_fields,
        payload.engine,
        payload.version,
    )


@router.post("/batch/parse")
def pipeline_parse_batch(payload: ParseBatchPayload) -> dict[str, Any]:
    return pipeline.parse_batch(
        payload.batch_no,
        payload.parser_rule_name,
        wait=payload.wait,
        operator=payload.operator,
    )


@router.get("/batch/parse-status")
def pipeline_parse_status(batch_no: str = Query(..., min_length=1)) -> dict[str, Any]:
    """查询批次解析尝试：区分已确认回执与仍在运行的尝试。"""
    return pipeline.get_parse_status(batch_no)


@router.post("/test/parse-delay")
def pipeline_set_parse_delay(payload: ParseDelayPayload) -> dict[str, Any]:
    """测试/演练用途：设置批次解析临界区延迟，复现并发提交窗口。"""
    pipeline.set_parse_delay(payload.batch_no, payload.seconds)
    return {"ok": True, "message": f"批次 {payload.batch_no} 解析延迟已设为 {payload.seconds}s"}


@router.post("/test/inject-duplicates")
def pipeline_inject_duplicates(payload: InjectDuplicatePayload) -> dict[str, Any]:
    """测试/演练用途：注入修复前形态的重复明细，验证清理与统计口径。"""
    return pipeline.inject_legacy_duplicates(payload.batch_no, payload.rule_version_key)


@router.get("/usage/stats")
def pipeline_usage_stats(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    """基于唯一血缘的明细用量统计。"""
    return pipeline.get_usage_stats(batch_no)


@router.get("/lineage/{lineage_id}/trace")
def pipeline_trace_lineage(lineage_id: int) -> dict[str, Any]:
    """从血缘逐行展开标准化明细及原始正文，核查重复数据残留。"""
    return pipeline.trace_lineage(lineage_id)


@router.get("/entity/{entity_id}/trace")
def pipeline_trace_entity(entity_id: int) -> dict[str, Any]:
    """从标准化明细逐行回查原始记录、血缘与同原始行的其他版本明细。"""
    return pipeline.trace_entity(entity_id)


@router.post("/cleanup/duplicates")
def pipeline_cleanup_duplicates(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    """清理历史重复写入残留，保留最早明细，其余标记为重复残留。"""
    return pipeline.cleanup_duplicate_entities(batch_no)


@router.post("/batch/recover")
def pipeline_recover_batch(batch_no: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.recover_batch(batch_no)


@router.post("/dedup/run")
def pipeline_run_dedup(payload: DedupPayload) -> dict[str, Any]:
    return pipeline.run_dedup(payload.dedup_key_fields, payload.window, payload.retain_strategy)


@router.post("/validate")
def pipeline_validate_entities(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    return pipeline.validate_entities(batch_no)


@router.post("/aggregation/refresh")
def pipeline_refresh_aggregation(payload: AggregationPayload) -> dict[str, Any]:
    return pipeline.refresh_aggregation(
        payload.view_name, payload.dim_field, payload.metric_field, payload.agg_func
    )


@router.post("/export/build")
def pipeline_build_export(payload: ExportPayload) -> dict[str, Any]:
    return pipeline.build_export(payload.view_name, payload.export_format, payload.filters)


@router.post("/watermark/advance")
def pipeline_advance_watermark(payload: WatermarkPayload) -> dict[str, Any]:
    return pipeline.advance_watermark(payload.source_id, payload.batch_no, payload.watermark_value)


@router.post("/replay/batch")
def pipeline_replay_batch(payload: ReplayPayload) -> dict[str, Any]:
    return pipeline.replay_batch(payload.history_batch_range, payload.new_parser_rule)


@router.post("/schema/evolve")
def pipeline_evolve_schema(payload: SchemaEvolvePayload) -> dict[str, Any]:
    return pipeline.evolve_schema(
        payload.version, payload.data_source, payload.field_defs, payload.backward_compatible
    )


@router.post("/full")
def pipeline_run_full(payload: FullPipelinePayload) -> dict[str, Any]:
    return pipeline.run_full_pipeline(
        payload.source_id, payload.time_window, payload.raw_logs, payload.parser_rule
    )


@router.get("/snapshot")
def pipeline_get_snapshot() -> dict[str, Any]:
    """获取所有模块的数据快照，用于跨模块数据流验证。"""
    from app.services.base import BaseModuleService
    from app.services import SERVICES
    snapshots: dict[str, Any] = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()
    return {"modules": snapshots}