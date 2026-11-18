"""任务拓扑领域服务：管理流式计算任务的DAG拓扑结构。

增强能力: 算子链校验、节点分发策略、故障转移评估、资源配额检查、关联stream_job

序号14修复：算子链节点改为按完整标识精确匹配唯一配置。
- 旧规则用子串包含，短名称会命中另一个名称更长的配置，部署结果引用错误模块状态；
- 新规则见 topology_binding：不做前缀/子串匹配，歧义即拒绝，节点状态不一致不发布；
- 重复部署同一拓扑复用同一 binding_key，运行版本与降级清单都带版本号，异常只恢复一次。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import topology_binding as binding_rules
from app.services.base import BaseModuleService
from app.store import store


class TaskTopologyService(BaseModuleService):
    MODULE = "task_topology"
    ENTITY = "拓扑配置"
    LIST_FIELDS = ["拓扑名称", "算子链", "并行度", "数据分发", "故障转移", "资源配额", "运行状态"]
    REQUIRED_FIELDS = ["拓扑名称", "算子链", "并行度"]
    STATUS_ORDER = ["草稿", "已部署", "运行中", "已停止"]
    ACTION_RULES = {"部署拓扑": "草稿", "停止拓扑": "运行中"}
    NEGATIVE_ACTIONS = []

    # ---- 内部工具 ----

    def _find_topology(self, topology_name: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("拓扑名称") == topology_name:
                return row
        return None

    def _pinned_bindings(self, topology_name: str) -> dict[str, str]:
        run = store._topology_runs.get(topology_name)
        if not run:
            return {}
        return dict(run.get("pinned_bindings", {}))

    # ---- 校验 ----

    def validate_topology_dag(self, topology_name: str) -> dict[str, Any]:
        """校验拓扑DAG的合法性：算子链节点数、并行度范围、资源配额。"""
        topo = self._find_topology(topology_name)
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        checks = []
        chain = binding_rules.split_chain(topo.get("算子链", ""))
        checks.append({
            "check": "chain_nodes",
            "ok": 2 <= len(chain) <= 10,
            "detail": f"算子链节点数: {len(chain)}",
        })

        try:
            parallelism = int(topo.get("并行度", "4"))
            checks.append({
                "check": "parallelism",
                "ok": 1 <= parallelism <= 64,
                "detail": f"并行度: {parallelism}",
            })
        except (ValueError, TypeError):
            checks.append({"check": "parallelism", "ok": False, "detail": "并行度格式错误"})

        quota = topo.get("资源配额", "")
        if quota:
            checks.append({
                "check": "resource_quota",
                "ok": any(unit in quota for unit in ("cpu", "mem", "gpu", "core")),
                "detail": f"资源配额: {quota}",
            })

        return {
            "ok": all(c["ok"] for c in checks),
            "topology": topology_name,
            "chain": chain,
            "checks": checks,
        }

    def inspect_node_bindings(self, topology_name: str) -> dict[str, Any]:
        """只读解析算子链 → 每个节点的唯一模块绑定，不写任何状态。

        拓扑工作台用它展示实际绑定；歧义节点会给出候选和改用完整标识的提示。
        """
        topo = self._find_topology(topology_name)
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        pinned = self._pinned_bindings(topology_name)
        resolutions = binding_rules.resolve_chain(topo.get("算子链", ""), pinned)
        ambiguous = [r for r in resolutions if r.reason == "ambiguous"]
        missing = [r for r in resolutions if r.reason == "missing"]
        invalid = [r for r in resolutions if r.reason == "invalid"]
        unavailable = [r for r in resolutions if r.binding and not r.binding.available]

        return {
            "ok": not (ambiguous or missing or invalid),
            "topology": topology_name,
            "chain": [r.node for r in resolutions],
            "nodes": [r.as_dict() for r in resolutions],
            "ambiguous": [r.node for r in ambiguous],
            "missing": [r.node for r in missing],
            "invalid": [r.node for r in invalid],
            "unavailable": [
                {"node": r.node, "status": r.binding.status if r.binding else ""}
                for r in unavailable
            ],
            "signature": binding_rules.chain_signature(resolutions),
            "run_version": (store._topology_runs.get(topology_name) or {}).get("version", 0),
        }

    # ---- 部署 ----

    def deploy_with_topology_check(
        self, topology_name: str,
    ) -> dict[str, Any]:
        """部署拓扑前按完整标识校验算子链，全部可发布才写运行版本。

        失败语义（序号14）：任一节点歧义、缺失、无效，或绑定到的配置状态
        不可用，都不发布运行版本，也不覆盖上一次已封存版本；接口返回
        完整节点明细而不是半成品部署结果。
        """
        with store.lock:
            topo = self._find_topology(topology_name)
            if topo is None:
                return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

            pinned = self._pinned_bindings(topology_name)
            chain_raw = topo.get("算子链", "")
            resolutions = binding_rules.resolve_chain(chain_raw, pinned)

            blocking = [
                r for r in resolutions
                if not r.ok or (r.binding is not None and not r.binding.available)
            ]
            node_status = [r.as_dict() for r in resolutions]

            if blocking:
                reasons = []
                for resolution in blocking:
                    if resolution.binding is not None:
                        reasons.append(
                            f"节点「{resolution.node}」绑定的 {resolution.binding.binding_key} "
                            f"状态为「{resolution.binding.status}」，不可发布"
                        )
                    else:
                        reasons.append(
                            f"节点「{resolution.node}」{resolution.detail or resolution.reason}"
                        )
                store.register_failure_once(
                    self.MODULE,
                    "部署拓扑",
                    "; ".join(reasons),
                    strategy="retry",
                    context={"topology": topology_name},
                    fingerprint=f"topology-deploy:{topology_name}:{binding_rules.chain_signature(resolutions)}",
                )
                return {
                    "ok": False,
                    "topology": topology_name,
                    "published": False,
                    "nodes": node_status,
                    "blocking_nodes": [item.node for item in blocking],
                    "error": "算子链节点未通过唯一绑定校验，未发布运行版本",
                    "reasons": reasons,
                }

            signature = binding_rules.chain_signature(resolutions)
            existing_run = store._topology_runs.get(topology_name)
            if existing_run and existing_run.get("signature") == signature:
                # 重复部署：绑定与批次都不变，返回同一个版本号，节点绑定不漂移。
                topo["status"] = "运行中"
                topo["pending"] = False
                topo["_runtime_signature"] = signature
                return {
                    "ok": True,
                    "topology": topology_name,
                    "published": False,
                    "idempotent": True,
                    "version": existing_run["version"],
                    "signature": signature,
                    "nodes": node_status,
                    "bindings": {
                        r.node: r.binding.binding_key for r in resolutions if r.binding
                    },
                    "message": "节点绑定与已发布运行版本一致，未产生新版本",
                }

            version = int((existing_run or {}).get("version", 0)) + 1
            bindings = {
                r.node: r.binding.binding_key for r in resolutions if r.binding is not None
            }
            run = {
                "topology": topology_name,
                "version": version,
                "signature": signature,
                "chain": [r.node for r in resolutions],
                "bindings": bindings,
                "pinned_bindings": dict(bindings),
                "nodes": node_status,
                "published_at": time.time(),
                "source_versions": {},
            }
            store._topology_runs[topology_name] = run
            topo["status"] = "运行中"
            topo["pending"] = False
            topo["abnormal"] = False
            topo["_runtime_signature"] = signature
            topo["_run_version"] = version

            return {
                "ok": True,
                "topology": topology_name,
                "published": True,
                "idempotent": False,
                "version": version,
                "signature": signature,
                "nodes": node_status,
                "bindings": bindings,
            }

    # ---- 运行数据 ----

    def get_runtime_dataset(self, topology_name: str) -> dict[str, Any]:
        """拓扑运行数据：已发布版本、节点绑定、以及派生出的源端映射。

        没有运行版本时返回明确的空态（published=False），而不是伪装成可用。
        """
        topo = self._find_topology(topology_name)
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        run = store._topology_runs.get(topology_name)
        if not run:
            return {
                "ok": True,
                "topology": topology_name,
                "published": False,
                "empty": True,
                "empty_reason": "该拓扑尚未发布运行版本，请先完成节点绑定校验",
                "nodes": [],
                "source_mappings": [],
            }

        source_mappings = []
        for node in run["chain"]:
            key = run["bindings"].get(node)
            if not key:
                continue
            module, _, config_id = key.partition("#")
            if module in binding_rules.NODE_MODULE_FIELDS:
                row = store.find(module, int(config_id))
                if row is None:
                    continue
                if module == "stream_source":
                    source_mappings.append({
                        "node": node,
                        "module": module,
                        "config_id": int(config_id),
                        "source": str(row.get("源端名称", "")),
                        "binding_key": key,
                    })

        return {
            "ok": True,
            "topology": topology_name,
            "published": True,
            "empty": False,
            "version": run["version"],
            "signature": run["signature"],
            "published_at": run["published_at"],
            "chain": run["chain"],
            "bindings": run["bindings"],
            "nodes": run["nodes"],
            "source_mappings": source_mappings,
            "source_version": run.get("source_versions", {}),
        }

    def evaluate_failover(self, topology_name: str) -> dict[str, Any]:
        """评估故障转移能力：检查拓扑中是否有备用调度路径。"""
        topo = self._find_topology(topology_name)
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        failover_strategy = topo.get("故障转移", "")
        restore_plans = [
            rp for rp in store.rows("restore_plan")
            if topology_name in str(rp.get("任务范围", ""))
        ]
        checkpoints = [
            cp for cp in store.rows("checkpoint_mgr")
            if cp.get("checkpoint_id") and cp.get("status") == "已启用"
        ]

        can_failover = bool(
            failover_strategy or len(restore_plans) > 0 or len(checkpoints) > 0
        )
        return {
            "ok": True,
            "topology": topology_name,
            "can_failover": can_failover,
            "strategy": failover_strategy,
            "restore_plans": len(restore_plans),
            "available_checkpoints": len(checkpoints),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["deployed"] = sum(1 for r in rows if r.get("status") == "已部署")
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["published_versions"] = {
            name: run["version"] for name, run in store._topology_runs.items()
        }
        return base
