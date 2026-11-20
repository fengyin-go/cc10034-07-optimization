"""结构历史数据集与同步计划的口径核对脚本。

验收场景：按库表和日期筛选出两页数据 → 翻到第二页时登记一条迟到更正日志 →
新查询检索同一时间段 → 核对新旧查询对应的列映射和变更日志数据。

运行：backend/.venv/bin/python tests/test_ddl_history.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import pipeline_bus
from app.services.ddl_tracker import DdlTrackerService
from app.services.schema_sync import SchemaSyncService
from app.store import store

PASSED: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"[失败] {label} {detail}")
    PASSED.append(label)
    print(f"[通过] {label}")


def reset_ddl_state() -> None:
    pipeline_bus.ddl_log.clear()
    pipeline_bus.ddl_index.clear()
    pipeline_bus.source_watermarks.clear()
    pipeline_bus.history_queries.clear()
    pipeline_bus.ddl_revision = 0
    store.rows("schema_sync").clear()


def mapping_of(plan: dict) -> dict[str, str | None]:
    return {m["source_column"]: m["target_type"] for m in plan["column_mapping"]}


def main() -> None:
    reset_ddl_state()
    tracker = DdlTrackerService()
    sync = SchemaSyncService()

    # ------------------------------------------------------------------
    # 1. 造数：dw.orders 五条历史变更；无关表（同名异库、同库异表）混入；
    #    另灌 60 条噪音把 orders 记录挤出"最近 50 条"旧窗口
    # ------------------------------------------------------------------
    base = [
        ("ALTER TABLE orders ADD COLUMN amount INT", "2026-09-01T10:00:00"),
        ("ALTER TABLE orders ADD COLUMN status VARCHAR(16)", "2026-09-02T10:00:00"),
        ("ALTER TABLE orders ADD COLUMN remark TEXT", "2026-09-03T10:00:00"),
        ("ALTER TABLE orders ADD COLUMN region VARCHAR(32)", "2026-09-04T10:00:00"),
        ("ALTER TABLE orders ADD COLUMN score BIGINT", "2026-09-05T10:00:00"),
    ]
    records = [
        tracker.record_ddl("dw", "orders", ddl, executed_at=ts, source="canal_dw")
        for ddl, ts in base
    ]
    tracker.record_ddl("ods", "orders", "ALTER TABLE orders ADD COLUMN ods_flag INT",
                       executed_at="2026-09-02T11:00:00", source="canal_ods")
    tracker.record_ddl("dw", "users", "ALTER TABLE users ADD COLUMN age INT",
                       executed_at="2026-09-02T12:00:00", source="canal_dw")
    for i in range(60):
        tracker.record_ddl("dw", "users", f"ALTER TABLE users ADD COLUMN noise_{i} INT",
                           executed_at=f"2026-09-06T{i % 24:02d}:{i % 60:02d}:00",
                           source="canal_dw")
    check("造数完成（dw.orders 五条 + 无关表 + 60 条噪音）",
          pipeline_bus.current_ddl_revision() == 67)
    # 来源水位先覆盖所查区间，后续计划校验才能落在「通过」
    pipeline_bus.advance_source_watermark("canal_dw", "2026-09-06T00:00:00")

    # ------------------------------------------------------------------
    # 2. 查询 Q1：按库表和日期筛选，size=2 翻两页
    # ------------------------------------------------------------------
    q1_p1 = tracker.query_structure_history(
        "dw", "orders", "2026-09-01", "2026-09-05", page=1, size=2)
    qid = q1_p1["query_id"]
    rev1 = q1_p1["revision"]
    check("Q1 第一页：完整表身份命中 5 条，无关表未混入",
          q1_p1["total"] == 5
          and [i["record_id"] for i in q1_p1["items"]]
          == [records[0].record_id, records[1].record_id]
          and all(i["database"] == "dw" and i["table"] == "orders"
                  for i in q1_p1["items"]))
    check("Q1 单表覆盖：60 条噪音挤出旧窗口后记录仍齐全",
          q1_p1["total"] == 5 and len(pipeline_bus.get_recent_ddls()) == 50)

    # ------------------------------------------------------------------
    # 3. 翻页间隙：登记一条迟到更正日志（更正第 2 条：status 实为 BIGINT）
    # ------------------------------------------------------------------
    late = tracker.record_ddl(
        "dw", "orders", "ALTER TABLE orders ADD COLUMN status BIGINT",
        executed_at="2026-09-02T10:00:00", source="canal_dw",
        corrects=records[1].record_id)
    check("迟到更正登记：生效时间落在区间历史内，登记版本晚于 Q1 窗口",
          late.executed_at.isoformat() == "2026-09-02T10:00:00"
          and late.revision > rev1)

    # ------------------------------------------------------------------
    # 4. Q1 第二页：成员与顺序不变，迟到日志不挤进已钉住的查询
    # ------------------------------------------------------------------
    q1_p2 = tracker.query_structure_history(
        "dw", "orders", "2026-09-01", "2026-09-05", page=2, size=2, query_id=qid)
    check("Q1 第二页：版本边界与第一页相同",
          q1_p2["revision"] == rev1)
    check("Q1 第二页：成员与顺序一致，迟到日志未跳进分页",
          q1_p2["total"] == 5
          and [i["record_id"] for i in q1_p2["items"]]
          == [records[2].record_id, records[3].record_id]
          and late.record_id not in [i["record_id"] for i in q1_p2["items"]])
    q1_p1_again = tracker.query_structure_history(
        "dw", "orders", "2026-09-01", "2026-09-05", page=1, size=2, query_id=qid)
    check("Q1 重取第一页：并发写入后分页边界未跳过变化",
          q1_p1_again["items"] == q1_p1["items"] and q1_p1_again["total"] == 5)

    # ------------------------------------------------------------------
    # 5. 新查询 Q2：同一时间段，迟到更正在新口径可见并顶替原记录
    # ------------------------------------------------------------------
    q2_p1 = tracker.query_structure_history(
        "dw", "orders", "2026-09-01", "2026-09-05", page=1, size=2)
    check("Q2 新查询：钉住更新的版本边界",
          q2_p1["revision"] > rev1)
    check("Q2 变更日志：更正顶替原记录，成员变化只发生在更正点",
          q2_p1["total"] == 5
          and [i["record_id"] for i in q2_p1["items"]]
          == [records[0].record_id, late.record_id])
    ds_old = tracker.structure_history_dataset(
        "dw", "orders", "2026-09-01", "2026-09-05", query_id=qid)
    ds_new = tracker.structure_history_dataset(
        "dw", "orders", "2026-09-01", "2026-09-05", query_id=q2_p1["query_id"])
    old_ids = [r["record_id"] for r in ds_old["records"]]
    new_ids = [r["record_id"] for r in ds_new["records"]]
    check("新旧变更日志核对：旧口径保留原记录，新口径追溯重述",
          records[1].record_id in old_ids and late.record_id not in old_ids
          and records[1].record_id not in new_ids and late.record_id in new_ids
          and [i for i in old_ids if i != records[1].record_id]
          == [i for i in new_ids if i != late.record_id])

    # ------------------------------------------------------------------
    # 6. 列映射核对：同一列 status 在新旧口径下结论不同
    # ------------------------------------------------------------------
    entry, missing = sync.create_entry({
        "任务名称": "orders同步", "源表": "dw.orders", "目标表": "dwd_orders",
        "类型转换": "直接映射", "同步模式": "增量", "冲突策略": "覆盖目标",
    })
    assert entry is not None and not missing
    entry_id = entry["id"]
    plan_old = sync.build_sync_plan(
        {**entry, "生效起始": "2026-09-01", "生效截止": "2026-09-05"},
        query_id=qid, entry_id=entry_id)
    plan_new = sync.build_sync_plan(
        {**entry, "生效起始": "2026-09-01", "生效截止": "2026-09-05"},
        query_id=q2_p1["query_id"])
    map_old, map_new = mapping_of(plan_old), mapping_of(plan_new)
    check("列映射核对：同一列在新旧口径下结论不同（VARCHAR→STRING / BIGINT）",
          map_old["status"] == "STRING" and map_new["status"] == "BIGINT")
    check("列映射核对：未更正的列在两口径下结论一致",
          all(map_old[c] == map_new[c] for c in ("amount", "remark", "region", "score")))
    check("变更清单与同步计划清单同一份范围：共用修订口径",
          plan_old["revision"] == rev1
          and plan_old["range"] == ds_old["range"]
          and [r["record_id"] for r in plan_old["change_manifest"]]
          == [r["record_id"] for r in ds_old["records"]])

    # ------------------------------------------------------------------
    # 7. 校验结果回写同步清单；已交付计划按当时口径重算
    # ------------------------------------------------------------------
    stored = store.find("schema_sync", entry_id)
    check("校验结果回写同步清单",
          stored["校验结果"] == "通过"
          and "status→STRING" in stored["列映射"]
          and stored["口径版本"] == rev1)
    plan_asof = sync.get_sync_plan(entry_id, mode="as_of")
    check("已交付计划按当时口径重算：结论与交付时一致",
          mapping_of(plan_asof)["status"] == "STRING"
          and plan_asof["revision"] == rev1)
    plan_latest = sync.get_sync_plan(entry_id, mode="latest")
    check("最新口径重算：迟到更正生效，同一列得出新结论",
          mapping_of(plan_latest)["status"] == "BIGINT"
          and plan_latest["revision"] > rev1)
    check("当时口径仍可复现：as_of 不受 latest 重算影响",
          mapping_of(sync.get_sync_plan(entry_id, mode="as_of"))["status"] == "STRING")
    deliveries = sync.get_sync_plan(entry_id, mode="history")["deliveries"]
    check("交付历史留存：两次交付各自口径可查可算",
          [d["revision"] for d in deliveries] == [rev1, plan_latest["revision"]]
          and mapping_of(sync.get_sync_plan(entry_id, mode="as_of",
                                            revision=plan_latest["revision"]))["status"]
          == "BIGINT")

    # ------------------------------------------------------------------
    # 8. 资料不全：查询区间超出来源水位 → 标注缺口，不当作没有改列
    # ------------------------------------------------------------------
    ds_gap = tracker.structure_history_dataset(
        "dw", "orders", "2026-09-01", "2026-09-10")
    check("资料缺口标注：未到来源列入清单，而非按无变更处理",
          ds_gap["coverage"]["complete"] is False
          and ds_gap["coverage"]["missing_sources"] == ["canal_dw"])
    plan_gap = sync.build_sync_plan(
        {"任务名称": "orders同步", "源表": "dw.orders", "目标表": "dwd_orders",
         "生效起始": "2026-09-01", "生效截止": "2026-09-10"},
        query_id=ds_gap["query_id"])
    check("资料不全的校验结论：状态为资料不全并说明未到来源",
          plan_gap["validation"]["status"] == "资料不全"
          and "canal_dw" in plan_gap["validation"]["note"])
    pipeline_bus.advance_source_watermark("canal_dw", "2026-09-11T00:00:00")
    ds_full = tracker.structure_history_dataset(
        "dw", "orders", "2026-09-01", "2026-09-10")
    check("水位推进后资料到齐",
          ds_full["coverage"]["complete"] is True
          and ds_full["coverage"]["missing_sources"] == [])

    # ------------------------------------------------------------------
    # 9. 兼容旧接口：track_temporal_ddl 与旧 schema_registry 路径仍可用
    # ------------------------------------------------------------------
    legacy = tracker.track_temporal_ddl("orders", "2026-09-01", "2026-09-05")
    check("旧接口 track_temporal_ddl 兼容（按表名跨库汇总原始日志）",
          len(legacy) == 7 and {item["database"] for item in legacy} == {"dw", "ods"}
          and late.record_id in {item["record_id"] for item in legacy})
    plan_fallback = sync.build_sync_plan({"任务名称": "旧式任务", "源表": "orders",
                                          "目标表": "t", "类型转换": "直接映射"})
    check("旧路径回退：无完整表身份时仍按 schema_registry 出计划",
          plan_fallback["mapping_count"] > 0)

    print(f"\n全部 {len(PASSED)} 项核对通过。")


if __name__ == "__main__":
    main()
