"""位点续做数据集业务规则。

流数据源批量暂停时，每个消费组/分区只会确认一次已提交 offset；本服务把整组批次的
处理结果写入同一份「位点续做数据集」（store 里的 offset_resume 表），并在其中保留
失败分区及其最后确认位置，供另一个业务页面（指标采集）读取与回写。

关键口径：
- 幂等：同一批次（idem_key 相同）重复提交只生效一次，只确认一个 offset。
- 中断：批次中某项失败即停在未完成边界，已完成分区保持确认位置不动，未完成（失败/
  待处理）分区的源状态随快照一起退回，源状态与续做数据集始终落在同一边界。
- 唯一位点：暂停前最后位点与批次检查点给出的位置不一致时，以当前生效的位点续做
  数据集中该分区的最后确认位置为准，保证同一分区不会重复消费。
- 竞态：所有写操作共用一把锁，并发请求只允许一项生效。
"""
from __future__ import annotations

import threading
from typing import Any

from app.store import store

MODULE = "offset_resume"
STREAMIN_MODULE = "streamin"

# 批次内单项处理结果。
ITEM_SUCCESS = "成功"
ITEM_FAILED = "失败"
ITEM_PENDING = "待处理"
ITEM_STATUSES = (ITEM_SUCCESS, ITEM_FAILED, ITEM_PENDING)

# 消费侧回写后允许落到的终态。
WRITABLE_RESULTS = (ITEM_SUCCESS, ITEM_FAILED)

# 源状态：批量暂停成功后落到的状态，以及允许进入批量暂停的状态。
PAUSED_STATUS = "已暂停"
RUNNING_STATUS = "运行中"


class OffsetResumeError(Exception):
    """批量暂停/回写无法继续时抛出，消息可直接回给页面。"""


class OffsetResumeNotFoundError(OffsetResumeError):
    """目标数据集或分区不存在，路由层按 404 返回。"""


class OffsetResumeService:
    def __init__(self) -> None:
        # 内存仓库是进程内共享状态，一把锁保证竞态请求只有一项能改写数据集。
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ 读取
    def list_batches(self, *, source_code: str | None = None) -> list[dict[str, Any]]:
        """列出位点续做数据集；可按源编号过滤，最新批次排在最前。"""
        with self._lock:
            rows = [dict(row) for row in store.rows(MODULE)]
        if source_code:
            rows = [row for row in rows if row.get("源编号") == source_code]
        rows.sort(key=lambda row: int(row.get("批次序号", 0)), reverse=True)
        return rows

    def get_batch(self, dataset_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = store.find(MODULE, dataset_id)
            return dict(row) if row is not None else None

    def effective_offsets(self) -> list[dict[str, Any]]:
        """当前生效位点口径：逐源逐分区给出唯一生效位点及其来源。

        暂停前最后位点与批次检查点冲突时，只认位点续做数据集里仍未完成（失败/待处理）
        分区的最后确认位置；已经成功确认的分区跟随源的最后确认位点。这样同一分区在
        任何页面读到的生效位点都只有一个，不会被重复消费。
        """
        with self._lock:
            result: list[dict[str, Any]] = []
            for source in store.rows(STREAMIN_MODULE):
                source_code = str(source.get("源编号", ""))
                group = str(source.get("消费组", ""))
                open_item = self._find_open_item(source_code)
                if open_item is not None:
                    result.append({
                        "源编号": source_code,
                        "消费组": group,
                        "分区": open_item["分区"],
                        "生效位点": open_item["最后确认位点"],
                        "位点口径": "位点续做数据集",
                        "批次数据集ID": open_item["批次数据集ID"],
                        "分区状态": open_item["处理结果"],
                    })
                else:
                    result.append({
                        "源编号": source_code,
                        "消费组": group,
                        "分区": 0,
                        "生效位点": source.get("最后确认位点", 0),
                        "位点口径": "源最后确认位点",
                        "批次数据集ID": None,
                        "分区状态": ITEM_SUCCESS,
                    })
            return result

    # ------------------------------------------------------------ 批量暂停
    def batch_pause(
        self,
        source_ids: list[int],
        *,
        idem_key: str,
        offsets: dict[int, int] | None = None,
        fail_simulation: set[int] | None = None,
    ) -> dict[str, Any]:
        """对一组流数据源执行批量暂停，整组结果一次性写入位点续做数据集。

        每个源按其消费组/分区逐项确认 offset；同 idem_key 的重复提交直接返回首次结果，
        不会二次确认。任一项失败即停在未完成边界：已确认分区保持不动，其余分区随源
        状态一起退回，并在续做数据集中保留失败分区与最后确认位置。
        """
        offsets = offsets or {}
        fail_simulation = fail_simulation or set()
        with self._lock:
            # 幂等：同一批次重复提交只生效一次，确认结果沿用首次，不再写 offset。
            existing = self._find_idempotent(idem_key)
            if existing is not None:
                return self._summary(existing, duplicated=True)

            sources = self._load_sources(source_ids)
            # 快照用于把未完成分区的源状态退回未完成边界。
            snapshot = [dict(row) for row in sources]

            items: list[dict[str, Any]] = []
            succeeded: list[dict[str, Any]] = []
            failed: list[dict[str, Any]] = []
            pending: list[dict[str, Any]] = []
            interrupted = False

            for source in sources:
                source_code = str(source["源编号"])
                last_confirmed = int(source.get("最后确认位点", 0) or 0)
                # 失败分区续做时从续做数据集里的最后确认位置起步，而不是重放整段。
                open_item = self._find_open_item(source_code)
                start_offset = (
                    int(open_item["最后确认位点"])
                    if open_item is not None
                    else last_confirmed
                )
                pause_offset = int(offsets.get(int(source["id"]), start_offset))

                item: dict[str, Any] = {
                    "源编号": source_code,
                    "消费组": str(source.get("消费组", "")),
                    "分区": int(source.get("分区数量", 0) or 0),
                    "暂停前最后位点": last_confirmed,
                    "批次检查点位点": pause_offset,
                    "最后确认位点": start_offset,
                    "处理结果": ITEM_PENDING,
                    "失败原因": None,
                }
                items.append(item)

                if interrupted:
                    # 边界之后的项保持待处理，源状态不动，留待下一批次续做。
                    pending.append(item)
                    continue

                if str(source.get("status")) != RUNNING_STATUS:
                    item["处理结果"] = ITEM_PENDING
                    item["失败原因"] = "源不在运行中，跳过本批次"
                    pending.append(item)
                    continue

                if int(source["id"]) in fail_simulation:
                    item["处理结果"] = ITEM_FAILED
                    item["失败原因"] = "分区offset确认失败，保留最后确认位置待续做"
                    failed.append(item)
                    interrupted = True
                    # 已完成分区保持不动；当前失败项与后续项一起退回未完成边界。
                    self._rollback_unfinished(snapshot, succeeded)
                    continue

                # 成功项：本批次只确认一次 offset，源状态落到已暂停。
                item["处理结果"] = ITEM_SUCCESS
                item["最后确认位点"] = pause_offset
                source["最后确认位点"] = pause_offset
                source["status"] = PAUSED_STATUS
                source["源状态"] = PAUSED_STATUS
                source["pending"] = False
                succeeded.append(item)

            dataset_id = self._next_id()
            # 明细与三个分桶共享同一份 item 对象，回写时按源编号定位到的就是分桶里
            # 的同一项，避免拷贝后状态改了、分桶仍旧的不一致。
            if failed:
                batch_status = "已中断"
            elif pending:
                batch_status = "待续做"
            else:
                batch_status = "已完成"
            record = {
                "id": dataset_id,
                "批次数据集ID": dataset_id,
                "批次序号": dataset_id,
                "幂等键": idem_key,
                "状态": batch_status,
                "成功项": succeeded,
                "失败项": failed,
                "待处理项": pending,
                "明细": items,
                "成功数": len(succeeded),
                "失败数": len(failed),
                "待处理数": len(pending),
                "源编号": ",".join(str(item["源编号"]) for item in items),
                "status": batch_status,
                "pending": bool(failed or pending),
                "abnormal": bool(failed),
            }
            store.rows(MODULE).append(record)
            return self._summary(record, duplicated=False)

    # ------------------------------------------------------------ 消费侧回写
    def writeback(
        self,
        dataset_id: int,
        source_code: str,
        *,
        result: str,
        last_offset: int | None = None,
    ) -> dict[str, Any]:
        """另一个业务页面（指标采集消费侧）向同一份位点续做数据集回写自身处理结果。

        竞态与重复回写只允许一项生效：已处于成功终态的分区不再被改写。
        """
        with self._lock:
            record = store.find(MODULE, dataset_id)
            if record is None:
                raise OffsetResumeNotFoundError(f"位点续做数据集 {dataset_id} 不存在或已归档")
            if result not in WRITABLE_RESULTS:
                raise OffsetResumeError(f"回写结果「{result}」不在允许范围：{'、'.join(WRITABLE_RESULTS)}")

            target = self._find_record_item(record, source_code)
            if target is None:
                raise OffsetResumeNotFoundError(f"批次 {dataset_id} 中找不到源 {source_code} 的分区")
            if target["处理结果"] == ITEM_SUCCESS:
                # 第一项已生效，后续竞态/重复回写直接拒绝，保证一个分区只确认一次。
                raise OffsetResumeError(f"源 {source_code} 分区已确认成功，回写不再重复生效")

            from_bucket = self._bucket_of(record, target)
            target["处理结果"] = result
            target["失败原因"] = None if result == ITEM_SUCCESS else target.get("失败原因") or "消费侧回写标记失败"
            if last_offset is not None:
                target["最后确认位点"] = int(last_offset)

            if from_bucket is not None:
                from_bucket.remove(target)
            record.setdefault("成功项" if result == ITEM_SUCCESS else "失败项", []).append(target)

            record["成功数"] = len(record["成功项"])
            record["失败数"] = len(record["失败项"])
            record["待处理数"] = len(record["待处理项"])
            if not record["失败项"] and not record["待处理项"]:
                record["状态"] = "已完成"
                record["status"] = "已完成"
                record["pending"] = False
                record["abnormal"] = False
            elif record["失败项"]:
                record["状态"] = "已中断"
                record["status"] = "已中断"
                record["pending"] = True
                record["abnormal"] = True
            else:
                record["状态"] = "待续做"
                record["status"] = "待续做"
                record["pending"] = True
            return dict(record)

    # -------------------------------------------------------------- 内部工具
    def _load_sources(self, source_ids: list[int]) -> list[dict[str, Any]]:
        if not source_ids:
            raise OffsetResumeError("批量暂停至少选择一个流数据源")
        seen: set[int] = set()
        sources: list[dict[str, Any]] = []
        for raw in source_ids:
            sid = int(raw)
            if sid in seen:
                continue
            seen.add(sid)
            source = store.find(STREAMIN_MODULE, sid)
            if source is None:
                raise OffsetResumeError(f"流数据源 {sid} 不存在或已归档")
            sources.append(source)
        return sources

    def _rollback_unfinished(
        self,
        snapshot: list[dict[str, Any]],
        succeeded: list[dict[str, Any]],
    ) -> None:
        """中断时退回未完成边界：已完成分区保持不动，其余源整体还原快照。"""
        done_codes = {item["源编号"] for item in succeeded}
        table = store.rows(STREAMIN_MODULE)
        for snap in snapshot:
            if str(snap.get("源编号")) in done_codes:
                continue
            current = next((row for row in table if int(row.get("id", 0)) == int(snap["id"])), None)
            if current is not None:
                current.update(snap)

    def _find_idempotent(self, idem_key: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("幂等键") == idem_key:
                return row
        return None

    def _find_open_item(self, source_code: str) -> dict[str, Any] | None:
        """取该源当前仍生效的未完成分区（失败优先，其次待处理），取最新批次。

        按批次从新到旧只看第一个触及该源的批次：续做批次成功后，旧批次里保留的
        失败/待处理项仅作历史留痕，不再是当前生效位点，避免同一分区被重复消费。
        """
        candidates = [
            row for row in store.rows(MODULE)
            if source_code in str(row.get("源编号", "")).split(",")
        ]
        candidates.sort(key=lambda row: int(row.get("批次序号", 0)), reverse=True)
        for record in candidates:
            for bucket in ("失败项", "待处理项"):
                for item in record.get(bucket, []):
                    if item.get("源编号") == source_code:
                        item["批次数据集ID"] = record["批次数据集ID"]
                        return item
            # 最新批次已包含该源且没有未完成项，说明已在更新的批次里收口，
            # 旧批次的历史失败项不再生效。
            if any(item.get("源编号") == source_code for item in record.get("明细", [])):
                return None
        return None

    @staticmethod
    def _find_record_item(record: dict[str, Any], source_code: str) -> dict[str, Any] | None:
        for item in record.get("明细", []):
            if item.get("源编号") == source_code:
                return item
        return None

    @staticmethod
    def _bucket_of(record: dict[str, Any], item: dict[str, Any]) -> list[dict[str, Any]] | None:
        for bucket in ("成功项", "失败项", "待处理项"):
            rows = record.get(bucket, [])
            if item in rows:
                return rows
        return None

    def _next_id(self) -> int:
        return max((int(row.get("id", 0)) for row in store.rows(MODULE)), default=0) + 1

    @staticmethod
    def _summary(record: dict[str, Any], *, duplicated: bool) -> dict[str, Any]:
        return {
            "批次数据集ID": record["批次数据集ID"],
            "幂等键": record["幂等键"],
            "状态": record["状态"],
            "重复提交": duplicated,
            "成功数": record["成功数"],
            "失败数": record["失败数"],
            "待处理数": record["待处理数"],
            "成功项": record["成功项"],
            "失败项": record["失败项"],
            "待处理项": record["待处理项"],
            "明细": record["明细"],
        }


offset_resume_service = OffsetResumeService()
