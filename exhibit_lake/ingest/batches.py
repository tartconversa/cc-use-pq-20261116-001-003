"""事件批次的分段入湖、可恢复提交和幂等接收。

一个来源批次由一到多个按顺序到达的段组成。每段单独落原始行并预校验，
但只有 ``seal`` 才把整批的合格行暴露为可发布的已接收事件；``stage`` 只读取
已接收事件，因此未封存的段不会以半成品形式出现在快照里。

同段重放：段号相同、内容相同 -> 返回该段已有结果，``replay=True``。
内容不一致的重放 -> ``Conflict``（请求键/段内容冲突）。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid

from exhibit_lake.catalog.registry import now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound, StateError
from exhibit_lake.ingest.validation import canonical, digest, event

MAX_SEGMENT_ROWS = 5000


class Batches:
    def __init__(self, store: Store):
        self.store = store

    # ------------------------------------------------------------------
    # legacy single-shot ingest: equivalent to open + one segment + seal
    # ------------------------------------------------------------------
    def ingest(self, source_id: str, contract_id: str, request_key: str, rows: list[object]) -> dict:
        require_id(source_id, "source_id")
        require_id(contract_id, "contract_id")
        require_id(request_key, "request_key")
        if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_SEGMENT_ROWS:
            raise InvalidInput("batch must contain between 1 and 5000 events")
        try:
            payload_hash = digest(rows)
        except (TypeError, ValueError):
            raise InvalidInput("batch must contain JSON-compatible finite values") from None
        with self.store.transaction() as conn:
            previous = Store.one(conn, "SELECT * FROM batches WHERE source_id=? AND request_key=?",
                                 (source_id, request_key))
            if previous:
                return self._replay_whole(conn, previous, contract_id, payload_hash)
            context = self._context(conn, source_id, contract_id)
            batch_id = uuid.uuid4().hex
            conn.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (batch_id, source_id, request_key, payload_hash, contract_id, "landing",
                          now(), None, 0, 0, 0))
            conn.execute("INSERT INTO batch_streams(batch_id,expected_rows,expected_payload_hash,opened_at) "
                         "VALUES(?,?,?,?)", (batch_id, len(rows), payload_hash, now()))
            self._land(conn, batch_id, 1, rows, context)
            self._seal(conn, batch_id)
            Store.audit(conn, "batch.committed", batch_id,
                        {"source_id": source_id, "valid": Store.one(
                            conn, "SELECT valid_rows AS n FROM batches WHERE batch_id=?", (batch_id,))["n"]},
                        now())
            return self._report(conn, batch_id, replay=False)

    # ------------------------------------------------------------------
    # segmented flow
    # ------------------------------------------------------------------
    def open(self, source_id: str, contract_id: str, request_key: str, *,
             expected_rows: int | None = None, expected_payload_hash: str | None = None) -> dict:
        require_id(source_id, "source_id")
        require_id(contract_id, "contract_id")
        require_id(request_key, "request_key")
        if expected_rows is not None and (type(expected_rows) is not int or expected_rows < 1):
            raise InvalidInput("expected_rows must be a positive integer")
        if expected_payload_hash is not None:
            if not isinstance(expected_payload_hash, str) or len(expected_payload_hash) != 64:
                raise InvalidInput("expected_payload_hash must be a sha256 hex digest")
            try:
                int(expected_payload_hash, 16)
            except ValueError:
                raise InvalidInput("expected_payload_hash must be a sha256 hex digest") from None
        with self.store.transaction() as conn:
            previous = Store.one(conn, "SELECT * FROM batches WHERE source_id=? AND request_key=?",
                                 (source_id, request_key))
            if previous:
                stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?",
                                   (previous["batch_id"],))
                if stream is None:
                    raise Conflict("request key already used for a non-segmented batch")
                if previous["contract_id"] != contract_id:
                    raise Conflict("request key already used for different content")
                if expected_rows is not None:
                    known = previous["total_rows"] if stream["sealed_at"] else stream["expected_rows"]
                    if known not in (None, expected_rows):
                        raise Conflict("request key already opened with a different row count")
                if expected_payload_hash is not None:
                    known = previous["payload_hash"] if stream["sealed_at"] else stream["expected_payload_hash"]
                    if known not in (None, expected_payload_hash):
                        raise Conflict("request key already opened with a different payload hash")
                if stream["abandoned_at"]:
                    raise StateError("request key batch was abandoned")
                return self._stream_report(conn, previous["batch_id"], replay=True)
            self._context(conn, source_id, contract_id)
            batch_id = uuid.uuid4().hex
            conn.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (batch_id, source_id, request_key, "", contract_id, "landing",
                          now(), None, 0, 0, 0))
            conn.execute("INSERT INTO batch_streams(batch_id,expected_rows,expected_payload_hash,opened_at) "
                         "VALUES(?,?,?,?)", (batch_id, expected_rows, expected_payload_hash, now()))
            Store.audit(conn, "batch.opened", batch_id,
                        {"source_id": source_id, "expected_rows": expected_rows}, now())
            return self._stream_report(conn, batch_id, replay=False)

    def append(self, batch_id: str, segment_no: int, rows: list[object]) -> dict:
        if type(segment_no) is not int or segment_no < 1:
            raise InvalidInput("segment_no must be a positive integer")
        if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_SEGMENT_ROWS:
            raise InvalidInput("segment must contain between 1 and 5000 events")
        try:
            segment_hash = digest(rows)
        except (TypeError, ValueError):
            raise InvalidInput("segment must contain JSON-compatible finite values") from None
        with self.store.transaction() as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
            if stream is None:
                raise StateError("batch is not open for segmentation")
            if stream["abandoned_at"]:
                raise StateError("batch was abandoned")
            previous = Store.one(conn, "SELECT * FROM batch_segments WHERE batch_id=? AND segment_no=?",
                                 (batch_id, segment_no))
            if previous:
                if previous["payload_hash"] != segment_hash:
                    raise Conflict("segment number already used for different content")
                return self._stream_report(conn, batch_id, replay=True, segment_no=segment_no)
            if stream["sealed_at"]:
                raise StateError("batch is already sealed")
            expected_no = Store.one(conn, "SELECT COALESCE(MAX(segment_no),0)+1 AS n FROM batch_segments "
                                    "WHERE batch_id=?", (batch_id,))["n"]
            if segment_no != expected_no:
                raise Conflict("segment arrived out of order",
                               details={"expected_segment_no": expected_no})
            context = self._context(conn, batch["source_id"], batch["contract_id"])
            self._land(conn, batch_id, segment_no, rows, context)
            Store.audit(conn, "batch.segment_landed", batch_id,
                        {"segment_no": segment_no, "rows": len(rows)}, now())
            return self._stream_report(conn, batch_id, replay=False, segment_no=segment_no)

    def seal(self, batch_id: str) -> dict:
        with self.store.transaction() as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
            if stream is None:
                raise StateError("batch is not open for segmentation")
            if stream["abandoned_at"]:
                raise StateError("batch was abandoned")
            if stream["sealed_at"]:
                return self._stream_report(conn, batch_id, replay=True)
            if not Store.one(conn, "SELECT 1 AS x FROM batch_segments WHERE batch_id=? LIMIT 1", (batch_id,)):
                raise StateError("cannot seal a batch with no landed segments")
            self._seal(conn, batch_id)
            Store.audit(conn, "batch.committed", batch_id,
                        {"source_id": batch["source_id"],
                         "valid": Store.one(conn, "SELECT valid_rows AS n FROM batches WHERE batch_id=?",
                                            (batch_id,))["n"]}, now())
            return self._stream_report(conn, batch_id, replay=False)

    def abandon(self, batch_id: str, reason: str | None = None) -> dict:
        if reason is not None and (not isinstance(reason, str) or len(reason) > 200):
            raise InvalidInput("reason must be text of at most 200 characters")
        with self.store.transaction() as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
            if stream is None:
                raise StateError("batch is not open for segmentation")
            if stream["abandoned_at"]:
                return self._stream_report(conn, batch_id, replay=True)
            if stream["sealed_at"]:
                raise StateError("sealed batches cannot be abandoned")
            conn.execute("UPDATE batch_streams SET abandoned_at=?,abandon_reason=? WHERE batch_id=?",
                         (now(), reason, batch_id))
            conn.execute("UPDATE batches SET status='abandoned' WHERE batch_id=?", (batch_id,))
            Store.audit(conn, "batch.abandoned", batch_id,
                        {"reason": reason, "rows_landed": batch["total_rows"]}, now())
            return self._stream_report(conn, batch_id, replay=False)

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def report(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            return self._stream_report(conn, batch_id, replay=False)

    def unfinished(self, source_id: str | None = None) -> dict:
        """List landed-but-unsealed batches so an operator can resume or abandon."""
        if source_id is not None:
            require_id(source_id, "source_id")
        with self.store.transaction(write=False) as conn:
            if source_id is not None and not Store.one(conn, "SELECT source_id FROM sources WHERE source_id=?",
                                                       (source_id,)):
                raise NotFound("source not found")
            sql = ("SELECT b.batch_id,b.source_id,b.contract_id,b.request_key,b.total_rows,b.received_at,"
                   "s.opened_at,s.expected_rows,s.expected_payload_hash,"
                   "(SELECT COALESCE(MAX(segment_no),0) FROM batch_segments g WHERE g.batch_id=b.batch_id) AS segments,"
                   "(SELECT COUNT(*) FROM pending_events p WHERE p.batch_id=b.batch_id) AS pending_rows "
                   "FROM batches b JOIN batch_streams s ON s.batch_id=b.batch_id "
                   "WHERE s.sealed_at IS NULL AND s.abandoned_at IS NULL")
            args: tuple = ()
            if source_id is not None:
                sql += " AND b.source_id=?"
                args = (source_id,)
            sql += " ORDER BY s.opened_at,b.batch_id"
            batches = Store.many(conn, sql, args)
        return {"source_id": source_id, "unfinished": batches}

    def stream(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            return self._stream_report(conn, batch_id, replay=False)

    def pending(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
            rejected = Store.many(conn, "SELECT row_no,code,detail FROM rejected_rows WHERE batch_id=? "
                                  "ORDER BY row_no", (batch_id,))
            staged = Store.one(conn, "SELECT COUNT(*) AS n,COALESCE(SUM(problem_code IS NOT NULL),0) AS bad "
                               "FROM pending_events WHERE batch_id=?", (batch_id,))
            return {"batch_id": batch_id, "status": batch["status"],
                    "sealed": stream is not None and stream["sealed_at"] is not None,
                    "abandoned": stream is not None and stream["abandoned_at"] is not None,
                    "abandon_reason": stream["abandon_reason"] if stream else None,
                    "segments": Store.many(conn, "SELECT segment_no,row_count,first_row_no,last_row_no,"
                                          "payload_hash,submitted_at FROM batch_segments WHERE batch_id=? "
                                          "ORDER BY segment_no", (batch_id,)),
                    "landed_rows": batch["total_rows"], "valid_rows": batch["valid_rows"],
                    "rejected_rows": batch["rejected_rows"], "rejections": rejected,
                    "pending_valid": staged["n"] - staged["bad"],
                    "pending_rejected": staged["bad"]}

    def raw(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid raw row page")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            rows = Store.many(conn, "SELECT r.row_no,r.payload_json,r.payload_hash,"
                              "a.event_id AS accepted_event_id,j.code AS rejected_code,"
                              "p.event_id AS pending_event_id,p.problem_code AS pending_code,"
                              "s.abandoned_at "
                              "FROM raw_rows r "
                              "LEFT JOIN accepted_events a ON a.batch_id=r.batch_id AND a.row_no=r.row_no "
                              "LEFT JOIN rejected_rows j ON j.batch_id=r.batch_id AND j.row_no=r.row_no "
                              "LEFT JOIN pending_events p ON p.batch_id=r.batch_id AND p.row_no=r.row_no "
                              "LEFT JOIN batch_streams s ON s.batch_id=r.batch_id "
                              "WHERE r.batch_id=? ORDER BY r.row_no LIMIT ? OFFSET ?",
                              (batch_id, limit, offset))
        result = []
        for row in rows:
            if row["accepted_event_id"] is not None:
                disposition = {"outcome": "accepted", "event_id": row["accepted_event_id"]}
            elif row["rejected_code"] is not None:
                disposition = {"outcome": "rejected", "code": row["rejected_code"]}
            elif row["abandoned_at"] is not None:
                disposition = {"outcome": "abandoned"}
                if row["pending_event_id"] is not None:
                    disposition["event_id"] = row["pending_event_id"]
            elif row["pending_event_id"] is not None or row["pending_code"] is not None:
                disposition = {"outcome": "pending",
                               "code": row["pending_code"],
                               "event_id": row["pending_event_id"]}
            else:
                disposition = {"outcome": "unclassified"}
            result.append({"row_no": row["row_no"], "payload": json.loads(row["payload_json"]),
                           "payload_hash": row["payload_hash"], "disposition": disposition})
        return {"batch_id": batch_id, "rows": result}

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _context(self, conn: sqlite3.Connection, source_id: str, contract_id: str) -> dict:
        source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
        if not source:
            raise NotFound("source not found")
        if not source["active"]:
            raise Conflict("source inactive")
        contract = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=? AND source_id=?",
                             (contract_id, source_id))
        if not contract:
            raise NotFound("contract not found for source")
        venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (source["venue_id"],))
        exhibits = {x["exhibit_id"]: x for x in
                    Store.many(conn, "SELECT * FROM exhibits WHERE venue_id=?", (source["venue_id"],))}
        return {"source": source, "contract": contract, "venue": venue, "exhibits": exhibits}

    def _land(self, conn: sqlite3.Connection, batch_id: str, segment_no: int,
              rows: list[object], context: dict) -> None:
        """Store raw rows and stage normalized/rejected outcomes in pending_events."""
        first_row_no = Store.one(conn, "SELECT COALESCE(MAX(row_no),0)+1 AS n FROM raw_rows WHERE batch_id=?",
                                 (batch_id,))["n"]
        for index, row in enumerate(rows):
            row_no = first_row_no + index
            body = canonical(row)
            row_hash = digest(row)
            conn.execute("INSERT INTO raw_rows VALUES(?,?,?,?)", (batch_id, row_no, body, row_hash))
            normalized, problem = event(row, context["contract"], context["source"],
                                        context["venue"], context["exhibits"])
            if normalized is not None:
                conn.execute("INSERT INTO pending_events(batch_id,row_no,event_id,source_id,venue_id,"
                             "exhibit_id,event_at,local_day,kind,value_num,category,visitor_hash,payload_hash) "
                             "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (batch_id, row_no, normalized["event_id"], normalized["source_id"],
                              normalized["venue_id"], normalized["exhibit_id"], normalized["event_at"],
                              normalized["local_day"], normalized["kind"], normalized["value_num"],
                              normalized["category"], normalized["visitor_hash"], normalized["payload_hash"]))
            else:
                conn.execute("INSERT INTO pending_events(batch_id,row_no,payload_hash,problem_code,problem_detail) "
                             "VALUES(?,?,?,?,?)", (batch_id, row_no, row_hash, problem["code"], problem["detail"]))
        last_row_no = first_row_no + len(rows) - 1
        conn.execute("INSERT INTO batch_segments VALUES(?,?,?,?,?,?,?)",
                      (batch_id, segment_no, digest(rows), len(rows), first_row_no, last_row_no, now()))
        conn.execute("UPDATE batches SET total_rows=total_rows+? WHERE batch_id=?", (len(rows), batch_id))

    def _seal(self, conn: sqlite3.Connection, batch_id: str) -> None:
        """Promote pending rows into accepted/rejected and make the batch whole."""
        pending_rows = Store.one(conn, "SELECT COUNT(*) AS n FROM pending_events WHERE batch_id=?",
                                 (batch_id,))["n"]
        stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
        if stream["expected_rows"] is not None and stream["expected_rows"] != pending_rows:
            raise Conflict("sealed row count differs from declared row count",
                           details={"expected_rows": stream["expected_rows"], "landed_rows": pending_rows})
        seal_hash = self._whole_digest(conn, batch_id)
        if stream["expected_payload_hash"] is not None and stream["expected_payload_hash"] != seal_hash:
            raise Conflict("sealed content differs from declared payload hash")
        valid = 0
        rejected = 0
        # Stream pending rows rather than materialising a large batch in memory.
        for raw in conn.execute("SELECT * FROM pending_events WHERE batch_id=? ORDER BY row_no", (batch_id,)):
            row = dict(raw)
            if row["problem_code"] is None:
                existing = Store.one(conn, "SELECT payload_hash,batch_id FROM accepted_events "
                                     "WHERE source_id=? AND event_id=?", (row["source_id"], row["event_id"]))
                if existing:
                    code = "duplicate_event" if existing["payload_hash"] == row["payload_hash"] else "event_conflict"
                    conn.execute("INSERT INTO rejected_rows VALUES(?,?,?,?)",
                                 (batch_id, row["row_no"], code, "event identity was already accepted"))
                    rejected += 1
                    continue
                conn.execute("INSERT INTO accepted_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (row["event_id"], row["source_id"], batch_id, row["row_no"], row["venue_id"],
                              row["exhibit_id"], row["event_at"], row["local_day"], row["kind"],
                              row["value_num"], row["category"], row["visitor_hash"], row["payload_hash"], None))
                valid += 1
            else:
                conn.execute("INSERT INTO rejected_rows VALUES(?,?,?,?)",
                             (batch_id, row["row_no"], row["problem_code"], row["problem_detail"]))
                rejected += 1
        conn.execute("DELETE FROM pending_events WHERE batch_id=?", (batch_id,))
        conn.execute("UPDATE batches SET status='committed',payload_hash=?,committed_at=?,valid_rows=?,"
                     "rejected_rows=? WHERE batch_id=?", (seal_hash, now(), valid, rejected, batch_id))
        conn.execute("UPDATE batch_streams SET sealed_at=? WHERE batch_id=?", (now(), batch_id))

    @staticmethod
    def _whole_digest(conn: sqlite3.Connection, batch_id: str) -> str:
        """Hash the flattened landed rows exactly as ``digest(all_rows)`` would.

        ``canonical(list)`` is ``"[" + ",".join(canonical(row)) + "]"``, so the
        whole-batch identity can be rebuilt from stored raw rows without ever
        materialising the batch in memory.
        """
        hasher = hashlib.sha256(b"[")
        first = True
        for raw in conn.execute("SELECT payload_json FROM raw_rows WHERE batch_id=? ORDER BY row_no", (batch_id,)):
            if not first:
                hasher.update(b",")
            hasher.update(raw["payload_json"].encode("utf-8"))
            first = False
        hasher.update(b"]")
        return hasher.hexdigest()

    def _replay_whole(self, conn: sqlite3.Connection, batch: dict, contract_id: str,
                      payload_hash: str) -> dict:
        if batch["contract_id"] != contract_id:
            raise Conflict("request key already used for different content")
        if batch["status"] == "committed":
            if batch["payload_hash"] != payload_hash:
                raise Conflict("request key already used for different content")
            return self._report(conn, batch["batch_id"], replay=True)
        stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch["batch_id"],))
        if stream is None or stream["abandoned_at"]:
            raise Conflict("request key already used for different content")
        if self._whole_digest(conn, batch["batch_id"]) != payload_hash:
            raise Conflict("request key already used for different content")
        # A resend of the complete batch after an interrupted segmented load is a
        # safe retry: finish the unsealed stream rather than duplicating rows.
        self._seal(conn, batch["batch_id"])
        Store.audit(conn, "batch.committed", batch["batch_id"],
                    {"source_id": batch["source_id"], "recovered": True,
                     "valid": Store.one(conn, "SELECT valid_rows AS n FROM batches WHERE batch_id=?",
                                        (batch["batch_id"],))["n"]}, now())
        return self._report(conn, batch["batch_id"], replay=False)

    def _report(self, conn: sqlite3.Connection, batch_id: str, *, replay: bool) -> dict:
        batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        rejected = Store.many(conn, "SELECT row_no,code,detail FROM rejected_rows WHERE batch_id=? ORDER BY row_no",
                              (batch_id,))
        return {"batch_id": batch_id, "source_id": batch["source_id"], "request_key": batch["request_key"],
                "status": batch["status"], "total_rows": batch["total_rows"], "valid_rows": batch["valid_rows"],
                "rejected_rows": batch["rejected_rows"], "rejections": rejected, "replay": replay}

    def _stream_report(self, conn: sqlite3.Connection, batch_id: str, *, replay: bool,
                       segment_no: int | None = None) -> dict:
        batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        stream = Store.one(conn, "SELECT * FROM batch_streams WHERE batch_id=?", (batch_id,))
        report = self._report(conn, batch_id, replay=replay)
        report.update({
            "segments": Store.many(conn, "SELECT segment_no,row_count,first_row_no,last_row_no,payload_hash,"
                                   "submitted_at FROM batch_segments WHERE batch_id=? ORDER BY segment_no",
                                   (batch_id,)),
            "expected_rows": stream["expected_rows"] if stream else None,
            "expected_payload_hash": stream["expected_payload_hash"] if stream else None,
            "opened_at": stream["opened_at"] if stream else None,
            "sealed": bool(stream and stream["sealed_at"]),
            "abandoned": bool(stream and stream["abandoned_at"]),
            "abandon_reason": stream["abandon_reason"] if stream else None,
            "sealed_at": stream["sealed_at"] if stream else None,
        })
        if segment_no is not None:
            report["segment_no"] = segment_no
        return report
