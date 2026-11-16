"""位点续做数据集验收测试（纯标准库断言，不依赖 pytest）。

覆盖：
1. STRE-0001~0003 offset 10/20/30 批量暂停，只确认前两项，第三项保留可续位置；
2. 同批次重复提交只生效一次、只确认一个 offset；
3. 批次中断时已完成分区不动，源状态与续做数据集一起退回未完成边界；
4. 续做数据集保留失败分区和最后确认位置；
5. 消费侧（指标采集页面）回写，竞态/重复回写只允许一项生效；
6. 暂停前最后位点与检查点冲突时，生效位点只认续做数据集，不重复消费；
7. 失败分区续做成功后整组批次收口。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.offset_resume import (  # noqa: E402
    ITEM_FAILED,
    ITEM_PENDING,
    ITEM_SUCCESS,
    OffsetResumeError,
    OffsetResumeService,
)
from app.store import Store  # noqa: E402


def fresh_service() -> tuple[OffsetResumeService, Store]:
    # 每个用例使用独立内存仓库，避免用例间污染。
    import app.services.offset_resume as module
    test_store = Store()
    original = module.store
    module.store = test_store
    return OffsetResumeService(), test_store


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  ✓ {message}")


def main() -> None:
    print("用例1：批量暂停只确认前两项，第三项保留可续位置")
    svc, test_store = fresh_service()
    res = svc.batch_pause(
        [1, 2, 3],
        idem_key="batch-001",
        offsets={1: 10, 2: 20, 3: 30},
        fail_simulation={3},
    )
    check(res["成功数"] == 2 and res["失败数"] == 1 and res["待处理数"] == 0, "批量结果区分成功2/失败1/待处理0")
    check([i["源编号"] for i in res["成功项"]] == ["STRE-0001", "STRE-0002"], "成功项为前两项")
    failed = res["失败项"][0]
    check(failed["源编号"] == "STRE-0003", "失败项为第三项")
    check(failed["最后确认位点"] == 30, "失败分区保留最后确认位置30")
    check(failed["处理结果"] == ITEM_FAILED, "失败分区处理结果为失败")
    sources = {row["源编号"]: row for row in test_store.rows("streamin")}
    check(sources["STRE-0001"]["status"] == "已暂停" and sources["STRE-0001"]["最后确认位点"] == 10, "已完成分区1保持已暂停且offset=10")
    check(sources["STRE-0002"]["status"] == "已暂停" and sources["STRE-0002"]["最后确认位点"] == 20, "已完成分区2保持已暂停且offset=20")
    check(sources["STRE-0003"]["status"] == "运行中", "失败分区源状态退回未完成边界（运行中）")
    check(sources["STRE-0003"]["最后确认位点"] == 30, "失败分区源位点未被推进，不会重复消费")

    print("用例2：同批次重复提交只生效一次，只确认一个offset")
    again = svc.batch_pause([1, 2, 3], idem_key="batch-001", offsets={1: 99, 2: 99, 3: 99})
    check(again["重复提交"] is True, "重复提交被识别为幂等重放")
    check(len(test_store.rows("offset_resume")) == 1, "续做数据集只写入一条批次记录")
    check(sources["STRE-0001"]["最后确认位点"] == 10, "重复提交未二次确认offset（仍为10）")
    check(again["失败项"][0]["最后确认位点"] == 30, "重放返回首次结果，失败分区位置不变")

    print("用例3：续做数据集保留失败分区和最后确认位置")
    batches = svc.list_batches()
    check(len(batches) == 1 and batches[0]["状态"] == "已中断", "数据集记录批次为已中断")
    detail = svc.get_batch(batches[0]["id"])
    kept = next(i for i in detail["明细"] if i["源编号"] == "STRE-0003")
    check(kept["处理结果"] == ITEM_FAILED and kept["最后确认位点"] == 30, "数据集中可核对失败分区与最后确认位置")
    check(kept["暂停前最后位点"] == 30 and kept["批次检查点位点"] == 30, "同时记录暂停前最后位点与批次检查点位点")

    print("用例4：消费侧回写结果，竞态/重复回写只允许一项生效")
    dataset_id = batches[0]["id"]
    record = svc.writeback(dataset_id, "STRE-0003", result=ITEM_SUCCESS, last_offset=35)
    moved = next(i for i in record["明细"] if i["源编号"] == "STRE-0003")
    check(moved["处理结果"] == ITEM_SUCCESS and moved["最后确认位点"] == 35, "消费侧回写成功并推进确认位置到35")
    check(record["失败数"] == 0 and record["状态"] == "已完成", "回写后批次收口为已完成")
    race_error = None
    try:
        svc.writeback(dataset_id, "STRE-0003", result=ITEM_FAILED, last_offset=40)
    except OffsetResumeError as exc:
        race_error = str(exc)
    check(race_error is not None and "不再重复生效" in race_error, "竞态重复回写被拒绝，只允许一项生效")
    check(moved["最后确认位点"] == 35, "被拒回写未改写已确认位置")

    print("用例5：生效位点冲突时只认当前续做数据集，同一分区不重复消费")
    svc2, store2 = fresh_service()
    # 构造暂停前最后位点与批次检查点不一致：检查点想确认到 31，但分区失败。
    res2 = svc2.batch_pause(
        [1, 2, 3],
        idem_key="batch-002",
        offsets={1: 11, 2: 21, 3: 31},
        fail_simulation={3},
    )
    check(res2["失败项"][0]["暂停前最后位点"] == 30 and res2["失败项"][0]["批次检查点位点"] == 31, "暂停前位点30与检查点31给出不同位置")
    effective = {row["源编号"]: row for row in svc2.effective_offsets()}
    check(effective["STRE-0003"]["生效位点"] == 30, "冲突时生效位点取续做数据集的最后确认位置30而非检查点31")
    check(effective["STRE-0003"]["位点口径"] == "位点续做数据集", "指标采集只能选择当前生效的续做数据集口径")
    check(effective["STRE-0001"]["生效位点"] == 11 and effective["STRE-0001"]["位点口径"] == "源最后确认位点", "已成功分区跟随源确认位点11")

    print("用例6：失败分区续做，新批次从最后确认位置起步且不重放")
    record3 = svc2.batch_pause([3], idem_key="batch-003", offsets={3: 31})
    check(record3["状态"] == "已完成" and record3["成功数"] == 1, "续做批次成功收口")
    src3 = store2.find("streamin", 3)
    check(src3["status"] == "已暂停" and src3["最后确认位点"] == 31, "失败分区续做后确认到31并暂停")
    effective2 = {row["源编号"]: row for row in svc2.effective_offsets()}
    check(effective2["STRE-0003"]["位点口径"] == "源最后确认位点", "无未完成项后生效位点回归源确认位点，不再重复消费")

    print("用例7：批次中断时后续待处理项随源状态一起退回")
    svc3, store3 = fresh_service()
    res3 = svc3.batch_pause(
        [1, 2, 3],
        idem_key="batch-004",
        offsets={1: 10, 2: 20, 3: 30},
        fail_simulation={2},
    )
    check(res3["成功数"] == 1 and res3["失败数"] == 1 and res3["待处理数"] == 1, "中断后区分成功/失败/待处理三类")
    src = {row["源编号"]: row for row in store3.rows("streamin")}
    check(src["STRE-0001"]["status"] == "已暂停", "边界前已完成分区保持不动")
    check(src["STRE-0002"]["status"] == "运行中" and src["STRE-0002"]["最后确认位点"] == 20, "失败分区退回运行中且位点保留20")
    check(src["STRE-0003"]["status"] == "运行中" and src["STRE-0003"]["最后确认位点"] == 30, "待处理分区一并退回未完成边界")
    pending_item = next(i for i in res3["待处理项"] if i["源编号"] == "STRE-0003")
    check(pending_item["处理结果"] == ITEM_PENDING, "待处理项在续做数据集中明确标记")

    print("用例8：并发竞态请求只允许一项生效")
    import threading
    svc4, store4 = fresh_service()
    errors: list[str] = []
    barrier = threading.Barrier(4)

    def worker(key: str) -> None:
        barrier.wait()
        try:
            svc4.batch_pause([1, 2, 3], idem_key=key, offsets={1: 10, 2: 20, 3: 30})
        except OffsetResumeError as exc:  # pragma: no cover - 本用例同键不报错
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=("race-key",)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    check(not errors, "并发请求均未产生异常")
    check(len(store4.rows("offset_resume")) == 1, "四个竞态请求只生成一条生效批次")
    check(store4.find("streamin", 1)["最后确认位点"] == 10, "竞态下offset只确认一次")

    print("用例9：非运行中源跳过产生待处理项时批次标记为待续做")
    svc5, store5 = fresh_service()
    # 先把源1暂停，使下一批次中它只能跳过为待处理；源2、3正常成功。
    store5.find("streamin", 1)["status"] = "已暂停"
    res5 = svc5.batch_pause([1, 2, 3], idem_key="batch-005", offsets={1: 10, 2: 20, 3: 30})
    check(res5["成功数"] == 2 and res5["失败数"] == 0 and res5["待处理数"] == 1, "待处理项与成功项区分清楚")
    batch5 = store5.rows("offset_resume")[0]
    check(batch5["状态"] == "待续做", "无失败但有待处理项时批次为待续做而非已完成")

    print("用例10：回写不存在的数据集/分区抛出未找到异常")
    not_found = None
    try:
        svc5.writeback(999, "STRE-0001", result=ITEM_SUCCESS)
    except OffsetResumeError as exc:
        not_found = type(exc).__name__
    check(not_found == "OffsetResumeNotFoundError", "数据集不存在抛出 OffsetResumeNotFoundError")
    from app.services.offset_resume import OffsetResumeNotFoundError
    missing_partition = None
    try:
        svc5.writeback(batch5["id"], "STRE-9999", result=ITEM_SUCCESS)
    except OffsetResumeNotFoundError:
        missing_partition = True
    check(missing_partition is True, "分区不存在同样按未找到处理")

    print("\n全部验收用例通过 ✔")


if __name__ == "__main__":
    main()
