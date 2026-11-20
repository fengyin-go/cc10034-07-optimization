"""DDL追踪领域服务：追踪和解析DDL变更对Schema的影响。

上游：从管道总线消费 DdlRecord 进行自动分析
下游：schema_sync(列级变更)、partition_sync(分区变更) 消费分析结果

检索口径（结构历史数据集）：
- 完整表身份 (库, 表) 选定索引桶，无关表不参与扫描；
- 生效区间 [生效起始, 生效截止] 过滤，登记版本号作为版本边界；
- 一次检索钉住一个版本边界，翻页期间成员与顺序稳定，
  并发写入的迟到日志不会跳进已钉住的分页；
- 更正记录按可见性顶替原始记录：边界内可见则追溯重述，
  迟到于边界则维持原结论（新旧查询可能对同一列得出不同结论）。
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import DdlRecord, parse_moment, pipeline_bus

# 列类型片段：DECIMAL(10,2) / VARCHAR(16) / INT 等
_TYPE_RE = r"(\w+(?:\s*\(\s*[\d\s,]+\))?)"

# 历史查询会话容量：超出后淘汰最早钉住的查询
_HISTORY_QUERY_CAP = 200


class DdlTrackerService(BaseModuleService):
    MODULE = "ddl_tracker"
    ENTITY = "DDL记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"]
    REQUIRED_FIELDS = ["记录ID", "变更数据库", "变更表名"]
    STATUS_ORDER = ["已记录", "已分析", "已通知", "已处理"]
    ACTION_RULES = {"分析变更": "已记录", "发送通知": "已分析", "完成处理": "已分析"}
    NEGATIVE_ACTIONS = []

    HISTORY_ARTIFACT = "结构历史数据集"

    # ------------------------------------------------------------------
    # DDL 解析与登记
    # ------------------------------------------------------------------
    def analyze_ddl_impact(self, ddl: DdlRecord) -> dict[str, Any]:
        """分析DDL变更对目标表的影响范围。

        解析 DDL 语句中的列变化、分区变化、索引变化，
        产出影响报告供 schema_sync 和 partition_sync 消费。
        """
        ddl_text = ddl.ddl_statement
        column_changes: list[dict[str, Any]] = []
        partition_changes: list[dict[str, Any]] = []

        # 检测 ADD COLUMN
        add_cols = re.findall(
            rf'ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+{_TYPE_RE}',
            ddl_text, re.IGNORECASE)
        for col_name, col_type in add_cols:
            column_changes.append({"action": "add", "column": col_name, "type": col_type})
            ddl.column_changes.append({"action": "add", "column": col_name, "type": col_type})

        # 检测 DROP COLUMN（COLUMN 关键字可省，但不得吃掉 DROP PARTITION）
        drop_cols = re.findall(
            r'DROP\s+(?!PARTITION\b)(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?(\w+)',
            ddl_text, re.IGNORECASE)
        for col_name in drop_cols:
            column_changes.append({"action": "drop", "column": col_name})
            ddl.column_changes.append({"action": "drop", "column": col_name})

        # 检测 MODIFY COLUMN：改名不变，类型更新
        modify_cols = re.findall(
            rf'MODIFY\s+(?:COLUMN\s+)?(\w+)\s+{_TYPE_RE}',
            ddl_text, re.IGNORECASE)
        for col_name, col_type in modify_cols:
            column_changes.append({"action": "modify", "column": col_name, "new_type": col_type})
            ddl.column_changes.append({"action": "modify", "column": col_name, "new_type": col_type})

        # 检测 CHANGE COLUMN：旧名换新名 + 类型
        change_cols = re.findall(
            rf'CHANGE\s+(?:COLUMN\s+)?(\w+)\s+(\w+)\s+{_TYPE_RE}',
            ddl_text, re.IGNORECASE)
        for old_name, new_name, col_type in change_cols:
            column_changes.append({
                "action": "modify", "column": new_name,
                "old_column": old_name, "new_type": col_type,
            })
            ddl.column_changes.append({
                "action": "modify", "column": new_name,
                "old_column": old_name, "new_type": col_type,
            })

        # 检测 ADD PARTITION
        add_part = re.findall(r'ADD\s+PARTITION', ddl_text, re.IGNORECASE)
        if add_part:
            partition_changes.append({"action": "add_partition"})
            ddl.partition_changes.append({"action": "add_partition"})

        # 检测 DROP PARTITION
        drop_part = re.findall(r'DROP\s+PARTITION\s+(\w+)', ddl_text, re.IGNORECASE)
        for part_name in drop_part:
            partition_changes.append({"action": "drop_partition", "name": part_name})
            ddl.partition_changes.append({"action": "drop_partition", "name": part_name})

        affected_cols = len(column_changes)
        ddl.affected_columns = affected_cols
        rollback = self.generate_rollback(ddl.ddl_statement)

        impact = {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "column_changes": column_changes,
            "partition_changes": partition_changes,
            "affected_columns": affected_cols,
            "rollback_statement": rollback,
            "has_schema_impact": len(column_changes) > 0,
            "has_partition_impact": len(partition_changes) > 0,
            "analyzed_at": datetime.now().isoformat(),
        }

        # 对 schema_sync 有影响的DDL，写入管道供冲突检测消费（兼容旧路径）
        if impact["has_schema_impact"]:
            for cc in column_changes:
                pipeline_bus.schema_registry.setdefault(ddl.table_name, []).append({
                    "column": cc.get("column"),
                    "action": cc.get("action"),
                    "type": cc.get("type", cc.get("new_type", "")),
                    "timestamp": datetime.now().isoformat(),
                })

        pipeline_bus.add_ddl(ddl)
        pipeline_bus.write_audit(
            "DDL分析", f"{ddl.database}.{ddl.table_name}",
            f"分析DDL影响：列变更{affected_cols}，分区变更{len(partition_changes)}",
            "ddl_tracker", "已分析"
        )
        return impact

    def record_ddl(
        self,
        database: str,
        table_name: str,
        ddl_stmt: str,
        executed_at: str | datetime | None = None,
        source: str = "manual",
        corrects: str | None = None,
    ) -> DdlRecord:
        """记录并分析一条DDL。

        executed_at 为生效时间，缺省取当前时刻；迟到日志传入历史生效时间，
        登记版本号仍按到达顺序分配，因此会晚于已钉住的查询窗口。
        corrects 指向被更正的历史记录，构成追溯重述的口径来源。
        """
        ddl = DdlRecord(
            record_id=f"ddl_{uuid.uuid4().hex[:8]}",
            database=database,
            table_name=table_name,
            ddl_statement=ddl_stmt,
            executed_at=parse_moment(executed_at) if executed_at is not None else datetime.now(),
            affected_columns=0,
            rollback_statement=self.generate_rollback(ddl_stmt),
            source=source,
            corrects=corrects,
        )
        self.analyze_ddl_impact(ddl)
        return ddl

    # ------------------------------------------------------------------
    # 结构历史数据集：版本钉住的生效区间检索
    # ------------------------------------------------------------------
    def structure_history_dataset(
        self,
        database: str,
        table_name: str,
        from_dt: str | datetime,
        to_dt: str | datetime,
        query_id: str | None = None,
        as_of_revision: int | None = None,
    ) -> dict[str, Any]:
        """把表结构变更日志按生效区间查询为结构历史数据集（全量记录）。

        query_id 复用已钉住的版本边界；as_of_revision 直接指定历史口径，
        用于已交付产物按当时口径重算。两者都不传则钉住当前版本边界。
        """
        ctx = self._resolve_query(database, table_name, from_dt, to_dt,
                                  query_id=query_id, as_of_revision=as_of_revision)
        records = pipeline_bus.query_ddl_history(
            ctx["database"], ctx["table"], ctx["from"], ctx["to"],
            as_of_revision=ctx["revision"])
        coverage = pipeline_bus.ddl_coverage(
            ctx["database"], ctx["table"], ctx["from"], ctx["to"])
        return {
            "artifact": self.HISTORY_ARTIFACT,
            "query_id": ctx["query_id"],
            "revision": ctx["revision"],
            "database": ctx["database"],
            "table": ctx["table"],
            "range": {"from": ctx["from"].isoformat(), "to": ctx["to"].isoformat()},
            "records": [self._serialize_record(ddl) for ddl in records],
            "total": len(records),
            "coverage": coverage,
        }

    def query_structure_history(
        self,
        database: str,
        table_name: str,
        from_dt: str | datetime,
        to_dt: str | datetime,
        page: int = 1,
        size: int = 20,
        query_id: str | None = None,
    ) -> dict[str, Any]:
        """结构历史数据集的分页视图。

        同一 query_id 的翻页共享钉住的版本边界：成员与顺序一致，
        分页边界不会因并发写入跳过变化。
        """
        dataset = self.structure_history_dataset(
            database, table_name, from_dt, to_dt, query_id=query_id)
        records = dataset.pop("records")
        total = len(records)
        start = (page - 1) * size
        dataset.update({
            "items": records[start:start + size],
            "total": total,
            "page": page,
            "size": size,
        })
        return dataset

    def _resolve_query(
        self,
        database: str,
        table_name: str,
        from_dt: str | datetime,
        to_dt: str | datetime,
        query_id: str | None,
        as_of_revision: int | None,
    ) -> dict[str, Any]:
        """解析查询上下文：复用已钉住的会话，或钉住新的版本边界。"""
        start = parse_moment(from_dt)
        end = parse_moment(to_dt, end_of_day=True)
        if query_id:
            ctx = pipeline_bus.history_queries.get(query_id)
            if ctx is None:
                raise ValueError(f"历史查询 {query_id} 不存在或已过期，请重新发起检索")
            if (ctx["database"], ctx["table"], ctx["from"], ctx["to"]) != \
                    (database, table_name, start, end):
                raise ValueError("查询条件与已钉住的历史查询不一致，请重新发起检索")
            return ctx
        revision = as_of_revision if as_of_revision is not None \
            else pipeline_bus.current_ddl_revision()
        ctx = {
            "query_id": f"q_{uuid.uuid4().hex[:10]}",
            "database": database,
            "table": table_name,
            "from": start,
            "to": end,
            "revision": revision,
            "created_at": datetime.now().isoformat(),
        }
        if as_of_revision is None:
            # 仅当前口径的查询登记会话供翻页复用；历史口径重算为一次性视图
            pipeline_bus.history_queries[ctx["query_id"]] = ctx
            while len(pipeline_bus.history_queries) > _HISTORY_QUERY_CAP:
                pipeline_bus.history_queries.pop(next(iter(pipeline_bus.history_queries)))
        return ctx

    @staticmethod
    def _serialize_record(ddl: DdlRecord) -> dict[str, Any]:
        return {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "ddl": ddl.ddl_statement,
            "executed_at": ddl.executed_at.isoformat(),
            "registered_at": ddl.registered_at.isoformat() if ddl.registered_at else None,
            "revision": ddl.revision,
            "source": ddl.source,
            "affected_columns": ddl.affected_columns,
            "column_changes": [dict(change) for change in ddl.column_changes],
            "corrects": ddl.corrects,
            "rollback_statement": ddl.rollback_statement,
        }

    # ------------------------------------------------------------------
    # 兼容旧接口
    # ------------------------------------------------------------------
    def track_temporal_ddl(self, table_name: str, from_dt: str, to_dt: str) -> list[dict[str, Any]]:
        """查询指定表在时间范围内的DDL变更序列（按表名跨库汇总）。"""
        results: list[dict[str, Any]] = []
        start = parse_moment(from_dt)
        end = parse_moment(to_dt, end_of_day=True)
        for (database, table), bucket in pipeline_bus.ddl_index.items():
            if table != table_name:
                continue
            for ddl in bucket:
                if start <= ddl.executed_at <= end:
                    results.append({
                        "record_id": ddl.record_id,
                        "database": database,
                        "ddl": ddl.ddl_statement[:200],
                        "executed_at": ddl.executed_at.isoformat(),
                        "affected_columns": ddl.affected_columns,
                    })
        results.sort(key=lambda item: item["executed_at"])
        return results

    def generate_rollback(self, ddl_stmt: str) -> str:
        """根据DDL语句生成回滚语句。"""
        ddl_upper = ddl_stmt.upper()
        if "ADD COLUMN" in ddl_upper:
            match = re.search(r'ADD\s+COLUMN\s+(\w+)', ddl_stmt, re.IGNORECASE)
            if match:
                return f"ALTER TABLE ... DROP COLUMN {match.group(1)};"
        if "DROP COLUMN" in ddl_upper:
            return "-- 回滚需从备份恢复列定义"
        if "MODIFY" in ddl_upper:
            return "-- 回滚需记录修改前的列类型"
        if "ADD PARTITION" in ddl_upper:
            return "ALTER TABLE ... DROP PARTITION ...;"
        return "-- 自动回滚生成中"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "发送通知":
            pipeline_bus.write_audit(
                "DDL通知", f"{entry.get('变更数据库', '')}.{entry.get('变更表名', '')}",
                f"DDL记录 {entry_id} 变更通知已发送",
                "ddl_tracker", "已通知"
            )
        return entry, msg
