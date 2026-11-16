"""流数据源接入接口：维护流数据源，覆盖启消费、暂停消费、关闭源等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    ActionResult,
    BatchPausePayload,
    EntryPayload,
    PageResult,
    ResumeWritebackPayload,
)
from app.services.offset_resume import (
    OffsetResumeError,
    OffsetResumeNotFoundError,
    offset_resume_service,
)
from app.services.streamin import StreaminService

router = APIRouter(prefix="/api/streamin", tags=["流数据源接入"])

service = StreaminService()

LIST_FIELDS = ["源编号", "源名称", "消息队列", "主题名称", "分区数量", "消费组", "起始位点", "源状态", "最后确认位点"]
STATUSES = ["草稿", "运行中", "已暂停", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按源编号检索"),
    status: str | None = Query(default=None, description="草稿、运行中、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按源编号与状态过滤流数据源接入列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/batch-pause")
def batch_pause(payload: BatchPausePayload) -> dict[str, Any]:
    """批量暂停：整组流数据源逐项确认 offset，结果一次性写入位点续做数据集。

    同一幂等键重复提交只生效一次且只确认一个 offset；批次中断时已完成分区保持不动，
    失败分区保留最后确认位置，源状态与续做数据集一起退回未完成边界。
    """
    try:
        summary = offset_resume_service.batch_pause(
            payload.source_ids,
            idem_key=payload.idem_key,
            offsets={int(key): value for key, value in payload.offsets.items()},
            fail_simulation=set(payload.fail_simulation),
        )
    except OffsetResumeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "message": "批量暂停已处理，结果写入位点续做数据集", "batch": summary}


@router.get("/resume")
def list_resume_batches(
    source_code: str | None = Query(default=None, description="按源编号过滤续做批次"),
) -> dict[str, Any]:
    """读取位点续做数据集：每个批次区分成功项、失败项与待处理项。"""
    items = offset_resume_service.list_batches(source_code=source_code)
    return {"module": "offset_resume", "total": len(items), "items": items}


@router.get("/resume/effective-offsets")
def effective_offsets() -> dict[str, Any]:
    """当前生效位点口径：暂停前最后位点与检查点冲突时只认续做数据集，杜绝重复消费。"""
    return {"items": offset_resume_service.effective_offsets()}


@router.get("/resume/{dataset_id}")
def get_resume_batch(dataset_id: int) -> dict[str, Any]:
    """读取单批次位点续做明细；不存在时给出可读说明。"""
    record = offset_resume_service.get_batch(dataset_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"位点续做数据集 {dataset_id} 不存在或已归档")
    return record


@router.post("/resume/{dataset_id}/writeback")
def resume_writeback(dataset_id: int, payload: ResumeWritebackPayload) -> dict[str, Any]:
    """消费侧回写自身处理结果到同一份位点续做数据集，竞态/重复回写只允许一项生效。"""
    try:
        record = offset_resume_service.writeback(
            dataset_id,
            payload.source_code,
            result=payload.result,
            last_offset=payload.last_offset,
        )
    except OffsetResumeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except OffsetResumeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "message": "消费侧结果已回写位点续做数据集", "entry": record}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条流数据源明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流数据源 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条流数据源，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流数据源已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条流数据源执行启消费、暂停消费、关闭源；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流数据源接入清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "streamin", "total": total, "items": items}
