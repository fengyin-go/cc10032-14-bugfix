"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.task_topology import TaskTopologyService
from app.services.throughput_mgr import ThroughputMgrService
from app.store import store

router = APIRouter(prefix="/api", tags=["业务资源"])

# 序号14 的两处异常页：拓扑工作台查看节点实际绑定，吞吐页面确认降级来源。
TOPOLOGY_ROUTER = APIRouter(prefix="/api/task_topology", tags=["任务拓扑"])
TOPOLOGY_RUNTIME_ROUTER = APIRouter(prefix="/api/topology_runtime", tags=["拓扑运行数据"])
THROUGHPUT_ROUTER = APIRouter(prefix="/api/throughput_mgr", tags=["吞吐管理"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---------------------------------------------------------------------------
# 序号14：拓扑节点绑定 / 运行数据 / 降级状态清单
# 这些固定路径必须先于 /api/{module}/{entry_id} 注册，否则会被动态段吞掉。
# ---------------------------------------------------------------------------

@TOPOLOGY_ROUTER.get("/{entry_id}/binding")
def topology_node_binding(entry_id: int) -> dict[str, Any]:
    """拓扑工作台：查看每个节点的实际模块绑定（只读，不写状态）。"""
    topo = store.find("task_topology", entry_id)
    if topo is None:
        raise HTTPException(status_code=404, detail=f"拓扑配置 {entry_id} 不存在或已归档")
    service = TaskTopologyService()
    return service.inspect_node_bindings(topo.get("拓扑名称", ""))


@TOPOLOGY_ROUTER.post("/{entry_id}/deploy")
def topology_deploy(entry_id: int) -> dict[str, Any]:
    """按完整标识校验算子链后发布运行版本；不通过时返回逐节点原因。"""
    topo = store.find("task_topology", entry_id)
    if topo is None:
        raise HTTPException(status_code=404, detail=f"拓扑配置 {entry_id} 不存在或已归档")
    service = TaskTopologyService()
    return service.deploy_with_topology_check(topo.get("拓扑名称", ""))


@TOPOLOGY_RUNTIME_ROUTER.get("/{topology_name}")
def topology_runtime(topology_name: str) -> dict[str, Any]:
    """拓扑运行数据：已发布版本、节点绑定、源端映射；未发布时返回空态。"""
    return TaskTopologyService().get_runtime_dataset(topology_name)


@THROUGHPUT_ROUTER.get("/degradation")
def throughput_degradation(
    topology: str = Query(..., min_length=1),
) -> dict[str, Any]:
    """读取当前降级状态清单（含来源拓扑运行版本）。"""
    return ThroughputMgrService().get_degradation_manifest(topology)


@THROUGHPUT_ROUTER.post("/degradation")
def throughput_build_degradation(
    topology: str = Query(..., min_length=1),
) -> dict[str, Any]:
    """依据拓扑运行数据生成降级状态清单；名称歧义时拒绝写入。"""
    return ThroughputMgrService().build_degradation_manifest(topology)
