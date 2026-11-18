"""序号14：任务拓扑算子链节点与模块配置的唯一绑定解析。

旧实现用 `node in str(row[前三个字段])` 做子串包含判断，短节点名会命中
另一个名称更长的配置（甚至命中 `status` / `pending` 这类非名称字段），
部署结果因而引用错误的模块状态，背压清单也随之落到错误分区。

本模块把匹配规则收紧为：

1. 只做完整标识精确匹配，不做子串 / 前缀包含；
2. 节点可以用 `module_key#配置ID` 或 `module_key:配置名称` 显式限定；
3. 裸名称必须命中唯一配置，命中多个配置（跨模块同名、同模块重复）视为
   歧义并拒绝绑定，提示改用完整模块键或配置 ID；
4. 已封存 / 已发布运行版本里的绑定会被钉住，重复部署同一拓扑复用同一
   binding_key，不会因为后来新增的相似配置而漂移。

绑定结果是确定性的：候选按 (模块, 配置ID) 排序，唯一命中才返回。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from app.store import store

# 算子链节点可能落在的模块，以及该模块的配置名称字段。
NODE_MODULE_FIELDS: dict[str, str] = {
    "stream_source": "源端名称",
    "calc_operator": "算子名称",
    "sliding_index": "指标名称",
    "window_config": "策略名称",
}

# 只有这些状态代表配置真正可用；草稿、已下线、停用、异常等都不参与发布。
AVAILABLE_STATUSES: dict[str, frozenset[str]] = {
    "stream_source": frozenset({"已接入"}),
    "calc_operator": frozenset({"已部署", "运行中"}),
    "sliding_index": frozenset({"计算中", "已生效"}),
    "window_config": frozenset({"已生效"}),
}


def split_chain(raw_chain: str) -> list[str]:
    """拆分算子链，兼容 `→` 与 `->`，保留节点原始标识顺序。"""
    text = str(raw_chain or "").replace("->", "→")
    return [item.strip() for item in text.split("→") if item.strip()]


@dataclass(frozen=True)
class NodeBinding:
    """一个算子链节点最终绑定的模块配置。"""

    node: str
    module: str
    config_id: int
    config_name: str
    status: str
    available: bool
    pinned: bool = False

    @property
    def binding_key(self) -> str:
        """完整标识：模块键 + 配置ID，跨模块唯一。"""
        return f"{self.module}#{self.config_id}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "module": self.module,
            "config_id": self.config_id,
            "config_name": self.config_name,
            "binding_key": self.binding_key,
            "status": self.status,
            "available": self.available,
            "pinned": self.pinned,
        }


@dataclass
class NodeResolution:
    """节点解析结果；reason 非空表示无法得到唯一绑定。"""

    node: str
    binding: NodeBinding | None = None
    reason: str = ""
    detail: str = ""
    candidates: list[NodeBinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.binding is not None and not self.reason

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "node": self.node,
            "found": self.binding is not None,
            "reason": self.reason,
            "detail": self.detail,
            "candidates": [c.as_dict() for c in self.candidates],
        }
        if self.binding is not None:
            payload.update(self.binding.as_dict())
            payload["binding"] = self.binding.as_dict()
        return payload


def _parse_identifier(raw: str) -> tuple[dict[str, Any] | None, str]:
    """解析节点标识：`module#id` / `module:name` / 裸名称。"""
    text = str(raw or "").strip()
    if not text:
        return None, "节点标识为空"

    if "#" in text:
        module_part, id_part = text.rsplit("#", 1)
        module = module_part.strip()
        if module not in NODE_MODULE_FIELDS:
            return None, f"模块限定符「{module or '空'}」不在算子链允许范围内"
        if not id_part.strip().isdigit():
            return None, f"配置ID「{id_part.strip()}」必须为正整数"
        return {"module": module, "config_id": int(id_part)}, ""

    if ":" in text:
        module_part, name_part = text.split(":", 1)
        module = module_part.strip()
        # 只有前缀确实是模块键时才当作限定标识，否则整体按名称处理。
        if module in NODE_MODULE_FIELDS:
            name = name_part.strip()
            if not name:
                return None, "模块限定符后缺少配置名称"
            return {"module": module, "name": name}, ""

    return {"module": None, "name": text}, ""


def _row_binding(node: str, module: str, row: dict[str, Any]) -> NodeBinding:
    name_field = NODE_MODULE_FIELDS[module]
    status = str(row.get("status", ""))
    return NodeBinding(
        node=node,
        module=module,
        config_id=int(row.get("id", 0)),
        config_name=str(row.get(name_field, "")),
        status=status,
        available=status in AVAILABLE_STATUSES[module],
    )


def _candidates(parsed: dict[str, Any], node: str) -> list[NodeBinding]:
    """只做精确匹配，并按 (模块, 配置ID) 排序保证结果确定性。"""
    modules = [parsed["module"]] if parsed.get("module") else list(NODE_MODULE_FIELDS)
    found: dict[tuple[str, int], NodeBinding] = {}
    for module in modules:
        name_field = NODE_MODULE_FIELDS[module]
        for row in store.rows(module):
            if parsed.get("config_id") is not None:
                if int(row.get("id", -1)) != parsed["config_id"]:
                    continue
            elif str(row.get(name_field, "")) != parsed["name"]:
                continue
            binding = _row_binding(node, module, row)
            found[(binding.module, binding.config_id)] = binding
    return [found[key] for key in sorted(found)]


def resolve_node(
    raw_node: str,
    pinned_key: str | None = None,
) -> NodeResolution:
    """把单个算子链节点解析成唯一配置绑定。"""
    node = str(raw_node or "").strip()
    parsed, error = _parse_identifier(node)
    if parsed is None:
        return NodeResolution(node=node, reason="invalid", detail=error)

    candidates = _candidates(parsed, node)
    if len(candidates) == 1:
        return NodeResolution(node=node, binding=candidates[0])

    if not candidates:
        return NodeResolution(
            node=node,
            reason="missing",
            detail=f"没有与「{node}」完整匹配的模块配置",
        )

    # 已发布运行版本钉住的绑定：只有当它仍是精确候选之一时才复用，
    # 这样重复部署不会因为后来新增的同名配置改变节点绑定。
    if pinned_key:
        for binding in candidates:
            if binding.binding_key == pinned_key:
                return NodeResolution(
                    node=node,
                    binding=NodeBinding(
                        node=binding.node,
                        module=binding.module,
                        config_id=binding.config_id,
                        config_name=binding.config_name,
                        status=binding.status,
                        available=binding.available,
                        pinned=True,
                    ),
                )

    keys = "、".join(binding.binding_key for binding in candidates)
    return NodeResolution(
        node=node,
        reason="ambiguous",
        detail=(
            f"「{node}」同时命中 {len(candidates)} 个配置（{keys}），"
            f"请改用「模块键#配置ID」或「模块键:配置名称」"
        ),
        candidates=candidates,
    )


def resolve_chain(
    chain: list[str] | str,
    pinned: dict[str, str] | None = None,
) -> list[NodeResolution]:
    """解析整条算子链；pinned 为上一次运行版本留下的节点绑定。"""
    nodes = split_chain(chain) if isinstance(chain, str) else [
        str(node).strip() for node in chain if str(node).strip()
    ]
    pinned = pinned or {}
    return [resolve_node(node, pinned.get(node)) for node in nodes]


def chain_signature(resolutions: list[NodeResolution]) -> str:
    """节点绑定签名：只由节点顺序与 binding_key 决定，用于部署幂等判断。"""
    material = "|".join(
        f"{resolution.node}=>{resolution.binding.binding_key}"
        if resolution.binding is not None
        else f"{resolution.node}=><unbound>"
        for resolution in resolutions
    )
    return hashlib.sha1(material.encode("utf-8")).hexdigest()[:16]


def prefix_collisions(names: list[str], universe: list[str]) -> list[str]:
    """列出清单内名称与已知名称里互为前缀、容易串区的组合。

    `names` 是本清单实际绑定的名称，`universe` 是全部候选名称（例如所有
    吞吐配置的数据源）。只有落在清单里的名称参与比较，避免把无关配置
    也报成风险。
    """
    collisions: list[str] = []
    seen = sorted({name for name in universe if name})
    listed = {name for name in names if name}
    for i, left in enumerate(seen):
        for right in seen[i + 1 :]:
            if left == right:
                continue
            if left.startswith(right) or right.startswith(left):
                if left in listed or right in listed:
                    collisions.append(f"{left} ↔ {right}")
    return collisions
