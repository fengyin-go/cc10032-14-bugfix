"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

固定路径路由（序号14 的拓扑绑定 / 运行数据 / 降级清单）必须先从
`app.main` 注册，否则会被 `/api/{module}/{entry_id}` 动态段抢先匹配。
"""
from __future__ import annotations

from app.routers import resource

# 通用资源路由：最后注册。
ROUTERS = [resource]

# 固定路径路由：先注册。
FIXED_ROUTERS = [
    resource.TOPOLOGY_ROUTER,
    resource.TOPOLOGY_RUNTIME_ROUTER,
    resource.THROUGHPUT_ROUTER,
]
