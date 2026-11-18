"""序号14：任务拓扑算子链唯一绑定 + 吞吐降级状态清单回归测试。

覆盖：
  - 短名称不再命中另一个名称更长的配置（前缀包含缺陷）；
  - 节点名称歧义时拒绝绑定 / 拒绝发布，提示改用完整模块键或配置 ID；
  - 节点状态不一致时不发布运行版本，接口不返回半成品结果；
  - 重复部署同一拓扑保持同一节点绑定，批次幂等；
  - 降级清单只依据运行数据的节点→源端映射，歧义时拒绝写入；
  - 清单保留来源版本；空态、并发重复异常只恢复一次。
"""
from __future__ import annotations

import copy
import threading

import pytest

from app.services import topology_binding as binding_rules
from app.services.task_topology import TaskTopologyService
from app.services.throughput_mgr import ThroughputMgrService
from app.store import store


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """每个用例用独立的内存表，避免用例之间互相污染。"""
    tables = copy.deepcopy(store._tables)
    runs = copy.deepcopy(store._topology_runs)
    manifests = copy.deepcopy(store._degradation_manifests)
    failures = copy.deepcopy(store._failures)
    monkeypatch.setattr(store, "_tables", tables)
    monkeypatch.setattr(store, "_topology_runs", runs)
    monkeypatch.setattr(store, "_degradation_manifests", manifests)
    monkeypatch.setattr(store, "_failures", failures)
    yield


def _seed_topology(name: str, chain: str) -> dict:
    row = {
        "id": max((int(r.get("id", 0)) for r in store.rows("task_topology")), default=0) + 1,
        "status": "草稿",
        "pending": False,
        "abnormal": False,
        "拓扑名称": name,
        "算子链": chain,
        "并行度": "4",
        "数据分发": "broadcast",
        "故障转移": "checkpoint",
        "资源配额": "cpu=2,mem=4Gi",
        "运行状态": "待部署",
    }
    store.rows("task_topology").append(row)
    return row


def _seed(module: str, values: dict, status: str) -> int:
    row = {"id": max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1}
    row.update(values)
    row["status"] = status
    row["pending"] = False
    row["abnormal"] = False
    store.rows(module).append(row)
    return row["id"]


# ---------------------------------------------------------------------------
# 1. 前缀名称不再串到另一个配置
# ---------------------------------------------------------------------------

class TestExactNodeBinding:
    def test_short_name_does_not_hit_longer_prefix_config(self):
        """「算子A」不能命中「算子AB」——旧实现的子串包含缺陷。"""
        short_id = _seed("calc_operator", {"算子名称": "算子A"}, "运行中")
        _seed("calc_operator", {"算子名称": "算子AB"}, "已停止")
        source_id = _seed("stream_source", {"源端名称": "源A"}, "已接入")
        _seed_topology("前缀拓扑", f"stream_source#{source_id} → calc_operator#{short_id}")

        result = TaskTopologyService().deploy_with_topology_check("前缀拓扑")
        assert result["ok"] is True, result
        assert result["bindings"][f"calc_operator#{short_id}"] == f"calc_operator#{short_id}"
        assert all(node["config_name"] != "算子AB" for node in result["nodes"])

    def test_bare_short_name_prefers_exact_match_over_prefix(self):
        _seed("calc_operator", {"算子名称": "算子A"}, "运行中")
        longer = _seed("calc_operator", {"算子名称": "算子AB"}, "运行中")
        _seed_topology("裸名拓扑", "算子A")

        inspected = TaskTopologyService().inspect_node_bindings("裸名拓扑")
        assert inspected["ok"] is True
        assert inspected["nodes"][0]["binding_key"] != f"calc_operator#{longer}"

    def test_ambiguous_short_name_is_refused(self):
        """两个模块都有同名配置时，裸名称必须判定为歧义而不是随便取第一个。"""
        _seed("calc_operator", {"算子名称": "汇总算子"}, "运行中")
        _seed("sliding_index", {"指标名称": "汇总算子"}, "已生效")
        _seed_topology("歧义拓扑", "汇总算子")

        inspected = TaskTopologyService().inspect_node_bindings("歧义拓扑")
        assert inspected["ok"] is False
        assert inspected["ambiguous"] == ["汇总算子"]
        assert "模块键#配置ID" in inspected["nodes"][0]["detail"]

        deployed = TaskTopologyService().deploy_with_topology_check("歧义拓扑")
        assert deployed["ok"] is False
        assert deployed["published"] is False
        assert deployed["nodes"][0]["reason"] == "ambiguous"

    def test_qualified_identifier_resolves_ambiguity(self):
        calc_id = _seed("calc_operator", {"算子名称": "汇总算子"}, "运行中")
        _seed("sliding_index", {"指标名称": "汇总算子"}, "已生效")
        _seed_topology("限定拓扑", "calc_operator:汇总算子")

        result = TaskTopologyService().deploy_with_topology_check("限定拓扑")
        assert result["ok"] is True
        assert result["bindings"]["calc_operator:汇总算子"] == f"calc_operator#{calc_id}"


# ---------------------------------------------------------------------------
# 2. 节点状态不一致不发布
# ---------------------------------------------------------------------------

class TestUnavailableNodeBlocksPublish:
    def test_stopped_node_blocks_publish_and_has_no_partial_run(self):
        source_id = _seed("stream_source", {"源端名称": "源X"}, "已接入")
        stopped = _seed("calc_operator", {"算子名称": "停机算子"}, "已停止")
        _seed_topology("半成品拓扑", f"stream_source#{source_id} → calc_operator#{stopped}")

        result = TaskTopologyService().deploy_with_topology_check("半成品拓扑")
        assert result["ok"] is False
        assert result["published"] is False
        assert result["blocking_nodes"] == [f"calc_operator#{stopped}"] or result["reasons"]
        # 不发布运行版本 → 运行数据保持空态，接口不会返回半成品
        runtime = TaskTopologyService().get_runtime_dataset("半成品拓扑")
        assert runtime["published"] is False
        assert runtime["nodes"] == []
        assert store._topology_runs.get("半成品拓扑") is None

    def test_deploy_does_not_overwrite_previous_version_on_failure(self):
        source_id = _seed("stream_source", {"源端名称": "源Y"}, "已接入")
        op_id = _seed("calc_operator", {"算子名称": "稳定算子"}, "运行中")
        _seed_topology("回退拓扑", f"stream_source#{source_id} → calc_operator#{op_id}")
        first = TaskTopologyService().deploy_with_topology_check("回退拓扑")
        assert first["ok"] is True

        store.find("calc_operator", op_id)["status"] = "已停止"
        second = TaskTopologyService().deploy_with_topology_check("回退拓扑")
        assert second["ok"] is False
        run = store._topology_runs["回退拓扑"]
        assert run["version"] == first["version"]
        assert run["bindings"]["calc_operator#" + str(op_id)] == f"calc_operator#{op_id}"


# ---------------------------------------------------------------------------
# 3. 重复部署幂等
# ---------------------------------------------------------------------------

class TestIdempotentDeploy:
    def test_redeploy_keeps_binding_and_version(self):
        source_id = _seed("stream_source", {"源端名称": "源Z"}, "已接入")
        op_id = _seed("calc_operator", {"算子名称": "幂等算子"}, "运行中")
        _seed_topology("幂等拓扑", f"stream_source#{source_id} → calc_operator#{op_id}")

        service = TaskTopologyService()
        first = service.deploy_with_topology_check("幂等拓扑")
        second = service.deploy_with_topology_check("幂等拓扑")

        assert first["ok"] and second["ok"]
        assert second["idempotent"] is True
        assert second["version"] == first["version"]
        assert second["bindings"] == first["bindings"]

    def test_new_similar_config_does_not_steal_pinned_binding(self):
        """发布后新增一个同名前缀配置，重复部署仍钉住原绑定。"""
        op_id = _seed("calc_operator", {"算子名称": "绑定算子"}, "运行中")
        _seed_topology("钉住拓扑", "绑定算子")
        first = TaskTopologyService().deploy_with_topology_check("钉住拓扑")
        assert first["ok"] is True

        # 新增同名配置制造歧义，但运行版本已钉住原绑定
        _seed("calc_operator", {"算子名称": "绑定算子"}, "运行中")
        inspected = TaskTopologyService().inspect_node_bindings("钉住拓扑")
        assert inspected["nodes"][0]["pinned"] is True
        assert inspected["nodes"][0]["binding_key"] == f"calc_operator#{op_id}"

        second = TaskTopologyService().deploy_with_topology_check("钉住拓扑")
        assert second["ok"] is True
        assert second["idempotent"] is True
        assert second["bindings"]["绑定算子"] == f"calc_operator#{op_id}"


# ---------------------------------------------------------------------------
# 4. 降级状态清单
# ---------------------------------------------------------------------------

class TestDegradationManifest:
    def _prepare(self, topology: str = "清单拓扑") -> tuple[int, int]:
        source_id = _seed("stream_source", {"源端名称": "订单流"}, "已接入")
        op_id = _seed("calc_operator", {"算子名称": "订单聚合"}, "运行中")
        _seed("throughput_mgr", {"配置名称": "订单吞吐", "数据源": "订单流",
                                  "最大QPS": "1000", "并发分区": "4",
                                  "背压阈值": "0.75", "限流策略": "token_bucket",
                                  "扩容规则": "double"}, "已生效")
        _seed_topology(topology, f"stream_source#{source_id} → calc_operator#{op_id}")
        assert TaskTopologyService().deploy_with_topology_check(topology)["ok"] is True
        return source_id, op_id

    def test_manifest_uses_runtime_mapping_and_source_version(self):
        self._prepare()
        result = ThroughputMgrService().build_degradation_manifest("清单拓扑")
        assert result["status"] == "ok"
        assert result["source_version"] == 1
        assert result["manifest_version"] == 1
        assert result["entries"][0]["source"] == "订单流"
        assert result["entries"][0]["binding_key"].startswith("stream_source#")
        assert result["manifest"]["source_version"] == 1

    def test_empty_runtime_returns_empty_state(self):
        _seed_topology("空态拓扑", "stream_source#1")
        result = ThroughputMgrService().build_degradation_manifest("空态拓扑")
        assert result["status"] == "empty"
        assert result["manifest"] is None
        assert result["entries"] == []

    def test_read_manifest_distinguishes_unpublished_from_not_generated(self):
        _seed_topology("空态拓扑2", "stream_source#1")
        unpublished = ThroughputMgrService().get_degradation_manifest("空态拓扑2")
        assert unpublished["status"] == "empty"
        assert unpublished["published"] is False
        assert "尚未发布" in unpublished["message"]

        source_id = _seed("stream_source", {"源端名称": "已发布源"}, "已接入")
        _seed_topology("已发布拓扑", f"stream_source#{source_id}")
        assert TaskTopologyService().deploy_with_topology_check("已发布拓扑")["ok"] is True
        not_generated = ThroughputMgrService().get_degradation_manifest("已发布拓扑")
        assert not_generated["status"] == "empty"
        assert not_generated["published"] is True
        assert "尚未生成" in not_generated["message"]

    def test_read_manifest_for_missing_topology(self):
        result = ThroughputMgrService().get_degradation_manifest("不存在的拓扑")
        assert result["ok"] is False
        assert result["status"] == "missing_topology"

    def test_manifest_reused_when_source_version_unchanged(self):
        self._prepare()
        service = ThroughputMgrService()
        first = service.build_degradation_manifest("清单拓扑")
        second = service.build_degradation_manifest("清单拓扑")
        assert second["status"] == "reused"
        assert second["manifest_version"] == first["manifest_version"]

    def test_manifest_refuses_similar_source_names(self):
        """互为前缀的两个源端名称不能静默挑一个写清单。"""
        source_id = _seed("stream_source", {"源端名称": "订单"}, "已接入")
        op_id = _seed("calc_operator", {"算子名称": "订单算子"}, "运行中")
        _seed("throughput_mgr", {"配置名称": "订单吞吐", "数据源": "订单",
                                  "最大QPS": "1000", "背压阈值": "0.75",
                                  "限流策略": "token_bucket", "扩容规则": "double"}, "已生效")
        _seed("throughput_mgr", {"配置名称": "订单明细吞吐", "数据源": "订单明细",
                                  "最大QPS": "800", "背压阈值": "0.75",
                                  "限流策略": "leaky_bucket", "扩容规则": "double"}, "已生效")
        _seed_topology("相似名称拓扑", f"stream_source#{source_id} → calc_operator#{op_id}")
        assert TaskTopologyService().deploy_with_topology_check("相似名称拓扑")["ok"] is True

        # 运行数据中的源端名称本身唯一，但吞吐配置里存在前缀相似名称时，
        # 精确匹配仍只取「订单」；再人为制造同名配置验证拒绝写入。
        first = ThroughputMgrService().build_degradation_manifest("相似名称拓扑")
        assert first["status"] == "ok"
        assert [e["source"] for e in first["entries"]] == ["订单"]
        assert first["similar_names"] == ["订单 ↔ 订单明细"]

        _seed("throughput_mgr", {"配置名称": "订单重复吞吐", "数据源": "订单",
                                  "最大QPS": "900", "背压阈值": "0.75",
                                  "限流策略": "token_bucket", "扩容规则": "double"}, "已生效")
        refused = ThroughputMgrService().build_degradation_manifest("相似名称拓扑")
        assert refused["status"] == "refused"
        assert refused["manifest"] is None
        assert "名称歧义" in refused["refusals"][0]["reason"]

    def test_manifest_version_increments_on_new_source_version(self):
        self._prepare()
        first = ThroughputMgrService().build_degradation_manifest("清单拓扑")
        # 拓扑运行版本推进（算子链变化导致签名变化）
        new_op = _seed("calc_operator", {"算子名称": "订单聚合2"}, "运行中")
        topo = next(r for r in store.rows("task_topology") if r["拓扑名称"] == "清单拓扑")
        source_binding = next(
            key for key in store._topology_runs["清单拓扑"]["bindings"].values()
            if key.startswith("stream_source#")
        )
        topo["算子链"] = f"{source_binding} → calc_operator#{new_op}"
        second_deploy = TaskTopologyService().deploy_with_topology_check("清单拓扑")
        assert second_deploy["version"] == 2
        second = ThroughputMgrService().build_degradation_manifest("清单拓扑")
        assert second["manifest_version"] == first["manifest_version"] + 1
        assert second["source_version"] == 2


# ---------------------------------------------------------------------------
# 5. 并发异常只恢复一次 / 幂等
# ---------------------------------------------------------------------------

class TestConcurrentExceptionRecovery:
    def test_duplicate_failure_registered_once(self):
        context = {"topology": "并发拓扑"}
        first = store.register_failure_once(
            "task_topology", "部署拓扑", "节点歧义", context=context,
            fingerprint="fp-1",
        )
        second = store.register_failure_once(
            "task_topology", "部署拓扑", "节点歧义", context=context,
            fingerprint="fp-1",
        )
        assert first.recovery_id == second.recovery_id
        assert len(store._failures) == 1

    def test_concurrent_manifest_writes_are_idempotent(self):
        source_id = _seed("stream_source", {"源端名称": "高并发流"}, "已接入")
        op_id = _seed("calc_operator", {"算子名称": "高并发算子"}, "运行中")
        _seed("throughput_mgr", {"配置名称": "高并发吞吐", "数据源": "高并发流",
                                  "最大QPS": "2000", "背压阈值": "0.75",
                                  "限流策略": "token_bucket", "扩容规则": "double"}, "已生效")
        _seed_topology("并发拓扑", f"stream_source#{source_id} → calc_operator#{op_id}")
        assert TaskTopologyService().deploy_with_topology_check("并发拓扑")["ok"] is True

        results: list[dict] = []
        lock = threading.Lock()

        def worker() -> None:
            out = ThroughputMgrService().build_degradation_manifest("并发拓扑")
            with lock:
                results.append(out)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        versions = {r["manifest_version"] for r in results if r["status"] in ("ok", "reused")}
        assert versions == {1}
        assert len(store._degradation_manifests["并发拓扑"]) == 1


# ---------------------------------------------------------------------------
# 6. 解析器边界
# ---------------------------------------------------------------------------

class TestResolverBoundaries:
    def test_chain_split_supports_ascii_arrow(self):
        assert binding_rules.split_chain("a -> b→c") == ["a", "b", "c"]

    def test_invalid_config_id_rejected(self):
        resolution = binding_rules.resolve_node("calc_operator#abc")
        assert resolution.reason == "invalid"

    def test_missing_module_rejected(self):
        resolution = binding_rules.resolve_node("unknown_module#1")
        assert resolution.reason == "invalid"

    def test_non_name_field_never_matches(self):
        """节点名不能靠 status / pending 等无关字段命中。"""
        _seed("calc_operator", {"算子名称": "真实算子"}, "运行中")
        resolution = binding_rules.resolve_node("运行中")
        assert resolution.binding is None


# ---------------------------------------------------------------------------
# 7. 路由顺序：固定路径不能被 /api/{module}/{entry_id} 抢先匹配
# ---------------------------------------------------------------------------

class TestRoutePrecedence:
    """走真实 ASGI 调度，确认固定路径没有被 {entry_id} 动态段吞掉。"""

    @staticmethod
    def _call(method: str, path: str) -> tuple[int, bytes]:
        import asyncio

        from app.main import app

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": b"topology=x",
            "headers": [(b"host", b"testserver")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        messages: list[dict] = []
        received = False

        async def receive() -> dict:
            nonlocal received
            if received:
                return {"type": "http.disconnect"}
            received = True
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message: dict) -> None:
            messages.append(message)

        asyncio.run(app(scope, receive, send))
        status = next(
            (m["status"] for m in messages if m["type"] == "http.response.start"), 0
        )
        body = b"".join(
            m.get("body", b"") for m in messages if m["type"] == "http.response.body"
        )
        return status, body

    def test_degradation_path_is_not_parsed_as_entry_id(self):
        status, body = self._call("GET", "/api/throughput_mgr/degradation")
        assert status == 200
        # 命中固定路由会返回清单契约；若被 {entry_id} 吞掉会因 int 校验报 422。
        assert b"manifest" in body

    def test_binding_path_is_not_parsed_as_entry_id(self):
        status, body = self._call("GET", "/api/task_topology/1/binding")
        assert status == 200
        assert b'"chain"' in body

    def test_generic_entry_lookup_still_works(self):
        status, body = self._call("GET", "/api/throughput_mgr/1")
        assert status == 200
        assert b"status" in body
