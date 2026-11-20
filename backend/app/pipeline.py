"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def parse_moment(value: str | datetime, *, end_of_day: bool = False) -> datetime:
    """归一化时间入参：支持日期串（按当日起点/终点）或完整 ISO 串。

    生效区间查询必须先把「日期」归一到边界时刻，否则字符串比较会把
    区间末端当天内的记录漏掉（如 '2026-09-05T10:00:00' > '2026-09-05'）。
    """
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return datetime.fromisoformat(
            f"{text}T23:59:59.999999" if end_of_day else f"{text}T00:00:00")
    return datetime.fromisoformat(text)


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。

    双时间轴：
    - executed_at   生效时间（valid time）：变更在源表生效的时刻，用于生效区间查询
    - registered_at 登记时间（transaction time）：日志进入中心的时刻
    - revision      登记版本号：由管道总线单调递增分配，作为查询/同步共用的版本边界
    迟到更正常见形态：executed_at 落在历史区间，revision 却晚于已钉住的查询窗口。
    """
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)
    registered_at: datetime | None = None
    revision: int = 0
    source: str = "manual"
    corrects: str | None = None  # 指向被更正记录的 record_id

    @property
    def identity(self) -> tuple[str, str]:
        """完整表身份：库 + 表，缺一则可能串到同名表。"""
        return (self.database, self.table_name)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # DDL 登记版本号：单调递增，作为查询与同步共用的版本边界
        self.ddl_revision: int = 0
        # DDL 按完整表身份 (库, 表) 建索引：检索只扫描目标表桶，不翻无关表
        self.ddl_index: dict[tuple[str, str], list[DdlRecord]] = defaultdict(list)
        # 来源水位：source -> 已连续到达的生效时间上限（ISO 字符串），用于识别资料缺口
        self.source_watermarks: dict[str, str | None] = {}
        # 已钉住版本边界的历史查询会话：query_id -> 查询上下文，
        # 跨服务共享（ddl_tracker 钉口径，schema_sync 按同一边界消费）
        self.history_queries: dict[str, dict[str, Any]] = {}
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录：分配登记版本号并按完整表身份入索引。"""
        self.ddl_revision += 1
        ddl.revision = self.ddl_revision
        if ddl.registered_at is None:
            ddl.registered_at = datetime.now()
        self.ddl_log.append(ddl)
        self.ddl_index[ddl.identity].append(ddl)
        self.source_watermarks.setdefault(ddl.source, None)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def current_ddl_revision(self) -> int:
        """当前登记版本号：查询开启时钉住它，翻页与同步都读同一边界。"""
        return self.ddl_revision

    def query_ddl_history(
        self,
        database: str,
        table_name: str,
        from_dt: datetime,
        to_dt: datetime,
        as_of_revision: int | None = None,
    ) -> list[DdlRecord]:
        """按完整表身份 + 生效区间查询 DDL 历史（结构历史数据集的底表）。

        - 只扫描 (database, table_name) 索引桶，无关表不参与过滤；
          单表记录全量覆盖，不受最近 N 条窗口截断。
        - as_of_revision 为版本边界：只可见 revision <= 边界的记录，
          并发写入的迟到日志不会挤进已钉住的查询。
        - 更正口径：可见的更正记录顶替其指向的原始记录（追溯重述）；
          更正晚于版本边界时，原始记录维持原结论。
        返回按 (生效时间, 登记版本) 排序的稳定序列。
        """
        revision_cap = self.ddl_revision if as_of_revision is None else as_of_revision
        bucket = self.ddl_index.get((database, table_name), [])
        visible = [
            ddl for ddl in bucket
            if ddl.revision <= revision_cap
            and from_dt <= ddl.executed_at <= to_dt
        ]
        superseded = {ddl.corrects for ddl in visible if ddl.corrects}
        effective = [ddl for ddl in visible if ddl.record_id not in superseded]
        effective.sort(key=lambda ddl: (ddl.executed_at, ddl.revision))
        return effective

    def advance_source_watermark(self, source: str, watermark: str | datetime) -> None:
        """推进来源水位：声明该来源 watermark 之前的生效区间已经到齐。"""
        self.source_watermarks[source] = parse_moment(watermark).isoformat()

    def ddl_coverage(self, database: str, table_name: str,
                     from_dt: datetime, to_dt: datetime) -> dict[str, Any]:
        """查询区间的资料到达情况。

        期望来源 = 该表身份出现过来报的来源。水位未覆盖到区间末端的来源
        记为「未到」：缺口不等于没有改列，下游不得按无变更处理。
        """
        expected = {
            ddl.source for ddl in self.ddl_index.get((database, table_name), [])
        }
        watermarks = {src: self.source_watermarks.get(src) for src in sorted(expected)}
        missing = [
            src for src, mark in watermarks.items()
            if mark is None or parse_moment(mark) < to_dt
        ]
        return {
            "complete": not missing,
            "missing_sources": missing,
            "watermarks": watermarks,
            "range": {"from": from_dt.isoformat(), "to": to_dt.isoformat()},
        }

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "ddl_revision": self.ddl_revision,
            "ddl_index_keys": len(self.ddl_index),
            "source_watermark_count": len(self.source_watermarks),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
        }


pipeline_bus = PipelineBus()