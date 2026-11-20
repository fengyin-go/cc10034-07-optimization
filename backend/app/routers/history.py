"""结构历史数据集与同步计划接口。

- GET  /api/ddl_tracker/history          按库表 + 生效区间检索结构历史数据集（分页）
- POST /api/ddl_tracker/history/register 登记 DDL 日志（支持迟到的历史生效时间与更正）
- GET  /api/schema_sync/{entry_id}/plan  读取/重算同步计划（stored|as_of|latest）

路由层只做参数收敛，口径与状态流转都在领域服务里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.ddl_tracker import DdlTrackerService
from app.services.schema_sync import SchemaSyncService

router = APIRouter(prefix="/api", tags=["结构历史"])


class DdlRegisterPayload(BaseModel):
    database: str
    table: str
    ddl: str
    executed_at: str | None = None
    source: str = "manual"
    corrects: str | None = None


class DdlRegisterResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] = Field(default_factory=dict)


def _tracker() -> DdlTrackerService:
    service = service_for("ddl_tracker")
    assert isinstance(service, DdlTrackerService)
    return service


def _sync() -> SchemaSyncService:
    service = service_for("schema_sync")
    assert isinstance(service, SchemaSyncService)
    return service


@router.get("/ddl_tracker/history")
def get_structure_history(
    database: str = Query(..., description="变更数据库"),
    table: str = Query(..., description="变更表名"),
    start: str = Query(..., description="生效起始（日期或ISO时间）"),
    end: str = Query(..., description="生效截止（日期或ISO时间）"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
    query_id: str | None = Query(default=None, description="翻页复用的已钉住查询"),
) -> dict[str, Any]:
    """结构历史数据集分页检索：同一 query_id 翻页共享版本边界。"""
    try:
        return _tracker().query_structure_history(
            database, table, start, end, page=page, size=size, query_id=query_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/ddl_tracker/history/register", response_model=DdlRegisterResult)
def register_ddl(payload: DdlRegisterPayload) -> DdlRegisterResult:
    """登记一条 DDL 日志：迟到日志传历史 executed_at，更正传 corrects。"""
    record = _tracker().record_ddl(
        payload.database, payload.table, payload.ddl,
        executed_at=payload.executed_at,
        source=payload.source,
        corrects=payload.corrects,
    )
    return DdlRegisterResult(
        ok=True,
        message=f"DDL日志已登记（版本 {record.revision}）",
        entry={
            "record_id": record.record_id,
            "revision": record.revision,
            "database": record.database,
            "table": record.table_name,
            "executed_at": record.executed_at.isoformat(),
            "registered_at": record.registered_at.isoformat() if record.registered_at else None,
            "source": record.source,
            "corrects": record.corrects,
        },
    )


@router.get("/schema_sync/{entry_id}/plan")
def get_sync_plan(
    entry_id: int,
    mode: str = Query(default="stored", description="stored|as_of|latest|history"),
    revision: int | None = Query(default=None, description="as_of 模式指定的交付版本"),
) -> dict[str, Any]:
    """同步计划清单：stored 当前交付，as_of 当时口径重算，latest 最新口径。"""
    try:
        return _sync().get_sync_plan(entry_id, mode=mode, revision=revision)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
