"""吞吐管理领域服务：管理流式计算任务的吞吐容量。

增强能力: 背压自动检测、QPS限流触发、扩容规则评估、降级策略验证、关联topology状态

序号14修复：降级状态清单只依据拓扑运行数据里的「节点 → 源端」绑定生成。
- 源端名称按完整标识精确匹配吞吐配置，不做前缀/子串匹配；
- 命中多个配置（含互为前缀的相似名称）时拒绝写入，并列出候选；
- 清单带来源版本：来源拓扑运行版本 + 本清单版本，重复生成不产生新版本；
- 并发/重复异常只登记一次，失败不落半成品清单。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import topology_binding as binding_rules
from app.services.base import BaseModuleService
from app.store import store


class ThroughputMgrService(BaseModuleService):
    MODULE = "throughput_mgr"
    ENTITY = "吞吐配置"
    LIST_FIELDS = ["配置名称", "数据源", "最大QPS", "并发分区", "背压阈值", "限流策略", "扩容规则"]
    REQUIRED_FIELDS = ["配置名称", "数据源", "最大QPS"]
    STATUS_ORDER = ["草稿", "已生效", "已触发", "已关闭"]
    ACTION_RULES = {"启用配置": "草稿", "关闭配置": "已生效"}
    NEGATIVE_ACTIONS = []

    # ---- 内部工具 ----

    @staticmethod
    def _configs_for_source(source_name: str) -> list[dict[str, Any]]:
        """精确匹配数据源名称；返回全部命中项，用于判定歧义。"""
        return [
            row for row in store.rows("throughput_mgr")
            if str(row.get("数据源", "")) == str(source_name)
        ]

    @staticmethod
    def _resolve_source_binding(source_name: str) -> tuple[dict[str, Any] | None, str]:
        """把一个源端名称解析成唯一吞吐配置。"""
        matches = ThroughputMgrService._configs_for_source(source_name)
        if not matches:
            return None, f"没有与源端「{source_name}」完整匹配的吞吐配置"
        if len(matches) > 1:
            keys = "、".join(
                f"{row.get('配置名称', '')}#{row.get('id', '')}" for row in matches
            )
            return None, (
                f"源端「{source_name}」命中 {len(matches)} 个吞吐配置（{keys}），"
                f"名称歧义，拒绝写入降级清单"
            )
        return matches[0], ""

    @staticmethod
    def _manifest_version(topology_name: str) -> int:
        manifests = store._degradation_manifests.get(topology_name, [])
        return int(manifests[-1]["manifest_version"]) if manifests else 0

    # ---- 背压监控 ----

    def monitor_backpressure(
        self, source_name: str, current_qps: float,
    ) -> dict[str, Any]:
        """监控背压状态：计算负载比、触发限流判断。"""
        config, reason = self._resolve_source_binding(source_name)
        if config is None:
            return {
                "ok": False,
                "source": source_name,
                "reason": reason,
                "overloaded": False,
            }
        try:
            max_qps = float(config.get("最大QPS", "1000"))
            threshold = float(config.get("背压阈值", "0.75"))
        except (ValueError, TypeError):
            max_qps, threshold = 1000.0, 0.75

        load_ratio = current_qps / max(max_qps, 1)
        is_overloaded = load_ratio > threshold

        if is_overloaded and config.get("status") != "已触发":
            config["status"] = "已触发"
            config["pending"] = True
            config["abnormal"] = True

        return {
            "ok": True,
            "source": source_name,
            "current_qps": current_qps,
            "max_qps": max_qps,
            "load_ratio": round(load_ratio, 4),
            "threshold": threshold,
            "overloaded": is_overloaded,
            "strategy": config.get("限流策略", "token_bucket"),
        }

    def apply_throttle_action(
        self, source_name: str,
    ) -> dict[str, Any]:
        """应用限流策略：计算降级后的QPS并写入状态存储。"""
        config, reason = self._resolve_source_binding(source_name)
        if config is None:
            return {"ok": False, "error": reason}

        try:
            max_qps = float(config.get("最大QPS", "1000"))
        except (ValueError, TypeError):
            max_qps = 1000.0

        strategy = config.get("限流策略", "token_bucket")
        if strategy == "token_bucket":
            new_limit = max_qps * 0.7
        elif strategy == "leaky_bucket":
            new_limit = max_qps * 0.5
        else:
            new_limit = max_qps * 0.8

        with store.lock:
            state_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                ) + 1,
                "配置名称": f"throttle_{config.get('配置名称', source_name)}",
                "后端类型": "memory",
                "状态键模式": f"throttle://{source_name}",
                "TTL时长": "3600",
                "检查点间隔": "30",
                "增量模式": "overwrite",
                "清理策略": "ttl_expire",
                "status": "已生效",
                "pending": True,
                "abnormal": True,
                "_throttle_source": source_name,
                "_throttle_config_id": config.get("id"),
                "_original_qps": max_qps,
                "_new_limit": new_limit,
                "_strategy": strategy,
                "_timestamp": time.time(),
            }
            store.rows("state_store").append(state_entry)

        return {
            "ok": True,
            "source": source_name,
            "config_id": config.get("id"),
            "strategy": strategy,
            "original_qps": max_qps,
            "new_limit": new_limit,
            "reduction_pct": round((1 - new_limit / max_qps) * 100, 1),
        }

    # ---- 降级状态清单 ----

    def build_degradation_manifest(self, topology_name: str) -> dict[str, Any]:
        """依据拓扑运行数据生成降级状态清单。

        返回结构区分三种终态：
          - empty：拓扑没有已发布运行版本，不写清单；
          - refused：源端名称歧义/无配置/状态不可用，拒绝写入；
          - ok：写入或复用一个带来源版本的清单版本。
        """
        with store.lock:
            # 延迟导入：避免服务注册表在导入期形成循环依赖。
            from app.services.task_topology import TaskTopologyService

            runtime = TaskTopologyService().get_runtime_dataset(topology_name)
            if not runtime.get("ok", False):
                return {
                    "ok": False,
                    "status": "missing_topology",
                    "topology": topology_name,
                    "error": runtime.get("error", "拓扑不存在"),
                }
            if not runtime.get("published"):
                return {
                    "ok": True,
                    "status": "empty",
                    "topology": topology_name,
                    "manifest": None,
                    "entries": [],
                    "message": runtime.get("empty_reason", "暂无运行数据"),
                }

            mappings = runtime.get("source_mappings", [])
            if not mappings:
                return {
                    "ok": True,
                    "status": "empty",
                    "topology": topology_name,
                    "source_version": runtime["version"],
                    "manifest": None,
                    "entries": [],
                    "message": "拓扑运行数据中没有源端节点，降级清单为空",
                }

            entries: list[dict[str, Any]] = []
            refusals: list[dict[str, Any]] = []
            for mapping in mappings:
                source_name = mapping["source"]
                config, reason = self._resolve_source_binding(source_name)
                if config is None:
                    refusals.append({
                        "node": mapping["node"],
                        "source": source_name,
                        "binding_key": mapping["binding_key"],
                        "reason": reason,
                    })
                    continue
                if config.get("status") == "已关闭":
                    refusals.append({
                        "node": mapping["node"],
                        "source": source_name,
                        "binding_key": mapping["binding_key"],
                        "reason": f"吞吐配置「{config.get('配置名称')}」已关闭，不可降级",
                    })
                    continue
                try:
                    max_qps = float(config.get("最大QPS", "1000"))
                except (ValueError, TypeError):
                    max_qps = 1000.0
                entries.append({
                    "node": mapping["node"],
                    "source": source_name,
                    "binding_key": mapping["binding_key"],
                    "throughput_config_id": config.get("id"),
                    "throughput_config_name": config.get("配置名称", ""),
                    "max_qps": max_qps,
                    "degraded_qps": round(max_qps * 0.7, 2),
                    "strategy": config.get("限流策略", "token_bucket"),
                })

            if refusals:
                store.register_failure_once(
                    self.MODULE,
                    "生成降级状态清单",
                    "; ".join(item["reason"] for item in refusals),
                    strategy="escalate",
                    context={
                        "topology": topology_name,
                        "refusals": refusals,
                        "source_version": runtime["version"],
                    },
                    fingerprint="degradation:" + "|".join(
                        sorted(f"{item['binding_key']}={item['reason']}" for item in refusals)
                    ),
                )
                return {
                    "ok": False,
                    "status": "refused",
                    "topology": topology_name,
                    "source_version": runtime["version"],
                    "manifest": None,
                    "entries": entries,
                    "refusals": refusals,
                    "error": "源端名称无法唯一绑定吞吐配置，已拒绝写入降级清单",
                }

            # 同名源端之间互为前缀时给出提示，便于在详情页确认没有串区。
            source_names = [entry["source"] for entry in entries]
            duplicates = binding_rules.prefix_collisions(
                source_names,
                [str(row.get("数据源", "")) for row in store.rows(self.MODULE)],
            )

            signature = "|".join(
                f"{entry['binding_key']}→{entry['throughput_config_id']}"
                for entry in entries
            )
            manifests = store._degradation_manifests.setdefault(topology_name, [])
            previous = manifests[-1] if manifests else None
            if (
                previous
                and previous.get("source_version") == runtime["version"]
                and previous.get("signature") == signature
            ):
                return {
                    "ok": True,
                    "status": "reused",
                    "topology": topology_name,
                    "source_version": runtime["version"],
                    "manifest_version": previous["manifest_version"],
                    "manifest": previous,
                    "entries": previous["entries"],
                    "similar_names": duplicates,
                    "message": "来源版本一致，复用已有降级清单",
                }

            manifest_version = int((previous or {}).get("manifest_version", 0)) + 1
            manifest = {
                "topology": topology_name,
                "manifest_version": manifest_version,
                "source_version": runtime["version"],
                "source_signature": runtime.get("signature", ""),
                "signature": signature,
                "effect": "degraded",
                "entries": entries,
                "created_at": time.time(),
            }
            manifests.append(manifest)

            return {
                "ok": True,
                "status": "ok",
                "topology": topology_name,
                "source_version": runtime["version"],
                "manifest_version": manifest_version,
                "manifest": manifest,
                "entries": entries,
                "similar_names": duplicates,
            }

    def get_degradation_manifest(self, topology_name: str) -> dict[str, Any]:
        """读取当前降级清单（只读）；无清单时区分「未发布」与「已发布未生成」。"""
        from app.services.task_topology import TaskTopologyService

        runtime = TaskTopologyService().get_runtime_dataset(topology_name)
        if not runtime.get("ok", False):
            return {
                "ok": False,
                "status": "missing_topology",
                "topology": topology_name,
                "manifest": None,
                "entries": [],
                "error": runtime.get("error", "拓扑不存在"),
            }

        manifests = store._degradation_manifests.get(topology_name, [])
        if not manifests:
            published = bool(runtime.get("published"))
            return {
                "ok": True,
                "status": "empty",
                "topology": topology_name,
                "source_version": runtime.get("version") if published else None,
                "manifest": None,
                "entries": [],
                "published": published,
                "message": (
                    "拓扑运行数据已发布，但尚未生成降级状态清单"
                    if published
                    else "该拓扑尚未发布运行版本，暂无降级状态数据"
                ),
            }
        manifest = manifests[-1]
        return {
            "ok": True,
            "status": "ok",
            "topology": topology_name,
            "manifest_version": manifest["manifest_version"],
            "source_version": manifest["source_version"],
            "published": True,
            "manifest": manifest,
            "entries": manifest["entries"],
        }

    def evaluate_auto_scale(self, source_name: str) -> dict[str, Any]:
        """评估自动扩容规则：判断是否需要扩容及扩容数量。"""
        config, reason = self._resolve_source_binding(source_name)
        if config is None:
            return {"ok": False, "error": reason}

        scale_rule = config.get("扩容规则", "").lower()
        try:
            current_partitions = int(config.get("并发分区", "4"))
        except (ValueError, TypeError):
            current_partitions = 4

        if "double" in scale_rule or "翻倍" in scale_rule:
            new_partitions = min(current_partitions * 2, 64)
        elif "incremental" in scale_rule or "增量" in scale_rule:
            new_partitions = min(current_partitions + 4, 64)
        else:
            new_partitions = current_partitions

        should_scale = config.get("status") == "已触发"

        return {
            "ok": True,
            "source": source_name,
            "should_scale": should_scale,
            "current_partitions": current_partitions,
            "new_partitions": new_partitions if should_scale else current_partitions,
            "scale_rule": scale_rule,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["triggered"] = sum(1 for r in rows if r.get("status") == "已触发")
        base["active"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["degradation_manifests"] = {
            name: manifests[-1]["manifest_version"]
            for name, manifests in store._degradation_manifests.items()
            if manifests
        }
        return base
