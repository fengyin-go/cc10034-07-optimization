"""Schema同步领域服务：管理源表到目标表的列级Schema同步。

上游：ddl_tracker 产出结构历史数据集 → schema_sync 消费并生成同步策略
下游：format_adapter 参考同步后的列映射进行格式适配

口径约定：
- 同步计划消费 ddl_tracker 的结构历史数据集，变更清单与同步计划清单
  取自同一份数据集（同一 query_id / 版本边界 / 生效区间），两处共用修订口径；
- 校验结果回写到同步清单（schema_sync 条目），已交付的计划保留口径版本，
  可按当时口径重算；迟到更正只影响新口径的计划，同一列可能得出不同结论；
- 资料不全的区间在校验中标注未到来源，不把缺口当作没有改列。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus
from app.store import store


class SchemaSyncService(BaseModuleService):
    MODULE = "schema_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源表", "目标表", "列映射", "类型转换", "同步模式", "冲突策略"]
    REQUIRED_FIELDS = ["任务名称", "源表", "目标表"]
    STATUS_ORDER = ["草稿", "同步中", "已完成", "同步失败"]
    ACTION_RULES = {"开始同步": "草稿", "完成同步": "同步中", "标记失败": "同步中"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 同步计划清单：entry_id -> 当前交付的计划（含口径版本）
        self._plans: dict[int, dict[str, Any]] = {}
        # 交付历史：entry_id -> 历次交付的计划，已交付的历史产物按各自口径留存
        self._plan_history: dict[int, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # 同步计划：消费结构历史数据集
    # ------------------------------------------------------------------
    def build_sync_plan(
        self,
        config: dict[str, Any],
        query_id: str | None = None,
        as_of_revision: int | None = None,
        entry_id: int | None = None,
    ) -> dict[str, Any]:
        """根据结构历史数据集构建同步计划。

        query_id 复用调用方已钉住的检索口径；as_of_revision 按指定历史口径
        重算；都不传则钉住当前版本边界。源表无法解析出完整表身份时，
        回退到旧的 schema_registry 路径以兼容既有调用。
        """
        source_table = config.get("源表", "")
        target_table = config.get("目标表", "")
        type_conversion = config.get("类型转换", "直接映射")
        conflict_strategy = config.get("冲突策略", "覆盖目标")
        database, table = self._split_identity(config.get("源数据库", ""), source_table)

        dataset: dict[str, Any] | None = None
        if database and table:
            from app.services import service_for
            tracker = service_for("ddl_tracker")
            dataset = tracker.structure_history_dataset(
                database, table,
                config.get("生效起始", "0001-01-01"),
                config.get("生效截止", "9999-12-31"),
                query_id=query_id, as_of_revision=as_of_revision)

        if dataset is not None:
            # 变更清单与同步计划清单取自同一份数据集：同范围、同口径
            change_manifest = dataset["records"]
            col_mapping = self.fold_column_mapping(change_manifest, type_conversion)
            revision = dataset["revision"]
            effective_range = dataset["range"]
            coverage = dataset["coverage"]
        else:
            source_cols = pipeline_bus.schema_registry.get(source_table, [])
            change_manifest = list(source_cols)
            col_mapping = []
            for col_info in source_cols:
                col_mapping.append({
                    "source_column": col_info.get("column"),
                    "source_type": col_info.get("type"),
                    "target_type": self.convert_type(col_info.get("type", "VARCHAR"), type_conversion),
                    "action": col_info.get("action"),
                })
            revision = pipeline_bus.current_ddl_revision()
            effective_range = None
            coverage = {"complete": True, "missing_sources": [], "watermarks": {}}

        validation = self.validate_mapping(col_mapping, coverage)
        plan = {
            "task_name": config.get("任务名称"),
            "source_table": source_table,
            "target_table": target_table,
            "revision": revision,
            "range": effective_range,
            "change_manifest": change_manifest,
            "column_mapping": col_mapping,
            "mapping_count": len(col_mapping),
            "coverage": coverage,
            "validation": validation,
            "conflict_strategy": conflict_strategy,
            "generated_ddl": self.generate_sync_ddl(col_mapping, target_table),
            "estimated_impact": self.estimate_sync_impact(col_mapping),
            "generated_at": datetime.now().isoformat(),
        }
        if entry_id is not None:
            self._plans[entry_id] = plan
            self._plan_history.setdefault(entry_id, []).append(plan)
            self._writeback_manifest(entry_id, plan)
        return plan

    def get_sync_plan(
        self,
        entry_id: int,
        mode: str = "stored",
        revision: int | None = None,
    ) -> dict[str, Any]:
        """读取同步计划。

        mode=stored 返回当前交付的计划（不存在则按当前口径生成）；
        mode=latest 按最新口径重算并再次交付；
        mode=as_of 按当时口径重算已交付产物：revision 指定某次交付的版本，
        缺省取最初交付的口径版本，迟到更正不改变历史结论；
        mode=history 列出全部交付版本。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            raise ValueError(f"{self.ENTITY} {entry_id} 不存在或已归档")
        stored = self._plans.get(entry_id)
        if mode == "stored":
            if stored is None:
                stored = self.build_sync_plan(entry, entry_id=entry_id)
            return stored
        if mode == "history":
            return {
                "entry_id": entry_id,
                "deliveries": [
                    {
                        "revision": p["revision"],
                        "generated_at": p["generated_at"],
                        "mapping_count": p["mapping_count"],
                        "validation": p["validation"]["status"],
                    }
                    for p in self._plan_history.get(entry_id, [])
                ],
            }
        if stored is None:
            raise ValueError(f"{self.ENTITY} {entry_id} 尚未交付同步计划，无法按{mode}口径重算")
        config = dict(entry)
        if stored.get("range"):
            config["生效起始"] = stored["range"]["from"]
            config["生效截止"] = stored["range"]["to"]
        if mode == "as_of":
            # 已交付的历史产物按当时口径重算：不覆盖已交付计划
            as_of = revision if revision is not None \
                else self._plan_history[entry_id][0]["revision"]
            return self.build_sync_plan(config, as_of_revision=as_of)
        if mode == "latest":
            return self.build_sync_plan(config, entry_id=entry_id)
        raise ValueError(f"未知重算模式：{mode}")

    def fold_column_mapping(
        self,
        change_manifest: list[dict[str, Any]],
        type_conversion: str,
    ) -> list[dict[str, Any]]:
        """把变更清单按生效顺序折叠成列映射（同步计划清单）。

        add/modify 更新列的末态类型，drop 移除列；同一列被更正时，
        数据集中可见的最后一条变更决定结论。
        """
        columns: dict[str, str] = {}
        dropped: set[str] = set()
        for record in change_manifest:
            for change in record.get("column_changes", []):
                action = change.get("action")
                name = change.get("column")
                if not name:
                    continue
                if action == "add":
                    columns[name] = change.get("type", "")
                    dropped.discard(name)
                elif action == "modify":
                    old_name = change.get("old_column")
                    if old_name and old_name != name:
                        columns.pop(old_name, None)
                    columns[name] = change.get("new_type") or change.get("type", "")
                    dropped.discard(name)
                elif action == "drop":
                    columns.pop(name, None)
                    dropped.add(name)
        mapping: list[dict[str, Any]] = [
            {
                "source_column": name,
                "source_type": col_type,
                "target_type": self.convert_type(col_type or "VARCHAR", type_conversion),
                "action": "keep",
            }
            for name, col_type in columns.items()
        ]
        for name in sorted(dropped):
            mapping.append({
                "source_column": name,
                "source_type": None,
                "target_type": None,
                "action": "drop",
            })
        return mapping

    def validate_mapping(
        self,
        col_mapping: list[dict[str, Any]],
        coverage: dict[str, Any],
    ) -> dict[str, Any]:
        """校验列映射并产出回写到同步清单的校验结果。

        资料不全优先于类型结论：缺口不等于没有改列，校验状态记为
        「资料不全」并列出未到来源，映射结果仅作 provisional 参考。
        """
        conflicts = [
            self.detect_type_conflict(
                m["source_column"], m["source_type"] or "", m["target_type"] or "")
            for m in col_mapping
            if m.get("action") != "drop" and m.get("source_type")
        ]
        # 无损转换（如 VARCHAR→STRING）只记录不阻断；需人工确认的才算冲突
        issues = [c for c in conflicts if c["conflict"] and not c.get("benign")]
        missing = coverage.get("missing_sources", [])
        if not coverage.get("complete", True):
            status = "资料不全"
            note = f"以下来源在所查区间未到齐：{'、'.join(missing)}；缺口不代表无改列"
        elif issues:
            status = "存在类型冲突"
            note = f"{len(issues)} 列存在类型冲突，需人工确认"
        else:
            status = "通过"
            note = "区间资料到齐，列映射可交付"
        return {
            "status": status,
            "note": note,
            "conflicts": conflicts,
            "checked_at": datetime.now().isoformat(),
        }

    def _writeback_manifest(self, entry_id: int, plan: dict[str, Any]) -> None:
        """把校验结果与列映射回写到同步清单条目。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        summary = "；".join(
            f"{m['source_column']}→{m['target_type']}"
            for m in plan["column_mapping"] if m.get("action") != "drop"
        )
        entry["列映射"] = summary or "（无列映射）"
        entry["校验结果"] = plan["validation"]["status"]
        entry["校验说明"] = plan["validation"]["note"]
        entry["口径版本"] = plan["revision"]
        if plan.get("range"):
            entry["生效区间"] = f"{plan['range']['from']}~{plan['range']['to']}"

    @staticmethod
    def _split_identity(database: str, source_table: str) -> tuple[str, str]:
        """从同步配置解析完整表身份：显式库名优先，其次 库.表 写法。"""
        if database:
            return database, source_table
        if "." in source_table:
            db, _, table = source_table.partition(".")
            return db.strip(), table.strip()
        return "", source_table

    # ------------------------------------------------------------------
    # 类型转换与影响评估
    # ------------------------------------------------------------------
    def convert_type(self, source_type: str, strategy: str) -> str:
        """类型转换策略执行。

        支持的转换：mysql->hive/postgresql/doris 等目标格式。
        """
        source_upper = source_type.upper()
        type_map = {
            "VARCHAR": {"直接映射": "STRING", "hive": "STRING", "doris": "VARCHAR(65533)"},
            "INT": {"直接映射": "INT", "hive": "INT", "doris": "INT"},
            "BIGINT": {"直接映射": "BIGINT", "hive": "BIGINT", "doris": "BIGINT"},
            "DECIMAL": {"直接映射": "DECIMAL(38,18)", "hive": "DECIMAL(38,18)", "doris": "DECIMAL(38,18)"},
            "DATETIME": {"直接映射": "TIMESTAMP", "hive": "TIMESTAMP", "doris": "DATETIME"},
            "TEXT": {"直接映射": "STRING", "hive": "STRING", "doris": "STRING"},
            "JSON": {"直接映射": "STRING", "hive": "STRING", "doris": "JSON"},
        }
        lower_strategy = strategy.lower()
        # 长类型名优先匹配，避免 INT 抢先吃掉 BIGINT
        for base in sorted(type_map, key=len, reverse=True):
            targets = type_map[base]
            if base in source_upper:
                return targets.get(lower_strategy, targets.get("直接映射", source_type))
        return "STRING"

    def estimate_sync_impact(self, col_mapping: list[dict[str, Any]]) -> dict[str, Any]:
        """估算同步对下游的影响范围。"""
        add_count = sum(1 for c in col_mapping if c.get("action") == "add")
        drop_count = sum(1 for c in col_mapping if c.get("action") == "drop")
        modify_count = sum(1 for c in col_mapping if c.get("action") == "modify")
        risk_level = "低"
        if drop_count > 0:
            risk_level = "高"
        elif modify_count > 0:
            risk_level = "中"
        return {
            "columns_added": add_count,
            "columns_dropped": drop_count,
            "columns_modified": modify_count,
            "risk_level": risk_level,
            "breaking_change": drop_count > 0,
        }

    def generate_sync_ddl(self, col_mapping: list[dict[str, Any]], target_table: str) -> str:
        """根据列映射生成目标DDL。"""
        cols: list[str] = []
        for cm in col_mapping:
            if cm.get("action") == "drop":
                continue
            cols.append(f"  {cm['source_column']} {cm['target_type']}")
        if not cols:
            return f"-- 无新增列，目标表 {target_table} 不变"
        return f"ALTER TABLE {target_table} ADD COLUMNS (\n" + ",\n".join(cols) + "\n);"

    def detect_type_conflict(self, col_name: str, source_type: str, target_type: str) -> dict[str, Any]:
        """检测列类型冲突：benign=True 表示无损转换，不阻断校验。"""
        source_upper = source_type.upper()
        target_upper = target_type.upper()
        has_conflict = source_upper != target_upper
        resolution = ""
        benign = False
        if has_conflict:
            if any(tok in source_upper for tok in ("VARCHAR", "CHAR", "TEXT")) \
                    and "STRING" in target_upper:
                resolution = "字符串宽度调整，无数据丢失"
                benign = True
            elif "DECIMAL" in source_upper and "BIGINT" in target_upper:
                resolution = "精度可能丢失，建议保留DECIMAL"
            else:
                resolution = "建议人工确认类型转换"
        return {
            "column": col_name,
            "source_type": source_type,
            "target_type": target_type,
            "conflict": has_conflict,
            "benign": benign,
            "resolution": resolution,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "开始同步":
            # 开始同步即按当前口径交付一份计划：消费结构历史数据集并回写清单
            self.build_sync_plan(entry, entry_id=entry_id)
            pipeline_bus.write_audit(
                "Schema同步", entry.get("源表", ""),
                f"同步计划已按口径版本 {self._plans[entry_id]['revision']} 交付并回写校验结果",
                "schema_sync", "同步中"
            )
        return entry, msg
