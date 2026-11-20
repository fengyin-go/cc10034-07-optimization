"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import history, resource

# history 含 /api/ddl_tracker/history 等固定路径，必须先于通用
# /api/{module}/{entry_id} 动态路由注册，否则会被抢先匹配。
ROUTERS = [history, resource]
