import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from exhibit_lake.api import create_app
from exhibit_lake.errors import Conflict, InvalidInput, NotFound, StateError
from exhibit_lake.service import ExhibitLake


STAMP = "2026-03-01T10:00:00+08:00"


class SegmentedIngestTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / "lake.db"
        self.lake = ExhibitLake(self.path)
        self.lake.register_venue("venue", "Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("visits", "venue", "visit")
        self.lake.register_contract("v1", "visits", ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")

    def visit(self, identity="e1", count=1):
        return {"event_id": identity, "exhibit_id": "robot", "event_at": STAMP, "count": count}

    def opened(self, key="batch", **options):
        return self.lake.open_batch("visits", "v1", key, **options)

    def published_events(self, day="2026-03-01"):
        return self.lake.snapshots.inventory("venue", day)["sources"]

    # ------------------------------------------------------------------
    # no half-baked data reaches snapshots before seal
    # ------------------------------------------------------------------
    def test_segments_are_invisible_until_seal(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a"), self.visit("b", 2)])
        self.assertEqual(self.published_events(), [])
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["landed_rows"], 2)
        with self.assertRaises(NotFound):
            self.lake.analytics.day("venue", "2026-03-01")
        sealed = self.lake.seal_batch(batch["batch_id"])
        self.assertEqual((sealed["valid_rows"], sealed["rejected_rows"]), (2, 0))
        self.assertEqual(self.published_events()[0]["events"], 2)
        snapshot = self.lake.publish_day("venue", "2026-03-01")
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["event_count"], 2)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01", snapshot["snapshot_id"])["event_count"], 2)

    def test_stage_before_seal_records_no_members(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        self.assertEqual(candidate["manifest"]["members"], [])
        self.lake.seal_batch(batch["batch_id"])
        published = self.lake.snapshots.publish(candidate["snapshot_id"], None)
        self.assertEqual(published["manifest"]["statistics"]["events"], 0)
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["unpublished_events"], 1)

    def test_rows_rejected_per_row_after_seal(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("ok", 3), self.visit("bad", 0)])
        self.lake.append_segment(batch["batch_id"], 2, [{"event_id": "missing"}])
        sealed = self.lake.seal_batch(batch["batch_id"])
        self.assertEqual((sealed["total_rows"], sealed["valid_rows"], sealed["rejected_rows"]), (3, 1, 2))
        self.assertEqual([x["row_no"] for x in sealed["rejections"]], [2, 3])
        self.assertEqual([x["code"] for x in sealed["rejections"]], ["invalid_count", "field_mismatch"])

    # ------------------------------------------------------------------
    # replay semantics: identical vs divergent
    # ------------------------------------------------------------------
    def test_identical_segment_replay_is_marked_replay(self):
        batch = self.opened()
        first = self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        again = self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        self.assertFalse(first["replay"])
        self.assertTrue(again["replay"])
        self.assertEqual(first["segments"], again["segments"])
        self.assertEqual(again["total_rows"], 1)

    def test_divergent_segment_replay_conflicts(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a", 1)])
        with self.assertRaises(Conflict):
            self.lake.append_segment(batch["batch_id"], 1, [self.visit("a", 9)])
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["landed_rows"], 1)

    def test_out_of_order_segment_rejected(self):
        batch = self.opened()
        with self.assertRaises(Conflict) as caught:
            self.lake.append_segment(batch["batch_id"], 2, [self.visit("a")])
        self.assertEqual(caught.exception.details["expected_segment_no"], 1)

    def test_open_replay_is_idempotent(self):
        first = self.opened()
        again = self.opened()
        self.assertEqual(first["batch_id"], again["batch_id"])
        self.assertTrue(again["replay"])
        self.assertEqual(again["total_rows"], 0)

    def test_open_replay_with_divergent_contract_conflicts(self):
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE contracts SET effective_to=? WHERE contract_id='v1'",
                         ("2026-12-31T00:00:00+00:00",))
        self.lake.register_contract("v2", "visits", ["event_id", "exhibit_id", "event_at", "count"],
                                    "2027-01-01T00:00:00+00:00")
        self.opened()
        with self.assertRaises(Conflict):
            self.lake.open_batch("visits", "v2", "batch")

    def test_whole_batch_key_is_shared_between_entry_points(self):
        first = self.lake.ingest("visits", "v1", "batch", [self.visit("a")])
        replay = self.lake.open_batch("visits", "v1", "batch")
        self.assertEqual(replay["batch_id"], first["batch_id"])
        self.assertTrue(replay["replay"])
        self.assertTrue(replay["sealed"])
        again = self.lake.ingest("visits", "v1", "batch", [self.visit("a")])
        self.assertEqual(again["batch_id"], first["batch_id"])
        self.assertTrue(again["replay"])
        with self.assertRaises(Conflict):
            self.lake.ingest("visits", "v1", "batch", [self.visit("a", 2)])

    def test_seal_replay_is_idempotent(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        first = self.lake.seal_batch(batch["batch_id"])
        again = self.lake.seal_batch(batch["batch_id"])
        self.assertFalse(first["replay"])
        self.assertTrue(again["replay"])
        self.assertEqual(self.published_events()[0]["events"], 1)

    def test_sealed_batch_replays_existing_segment_only(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        self.lake.seal_batch(batch["batch_id"])
        self.assertTrue(self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])["replay"])
        with self.assertRaises(Conflict):
            self.lake.append_segment(batch["batch_id"], 1, [self.visit("a", 2)])
        with self.assertRaises(StateError):
            self.lake.append_segment(batch["batch_id"], 2, [self.visit("b")])

    # ------------------------------------------------------------------
    # restart / recovery
    # ------------------------------------------------------------------
    def test_unfinished_batch_survives_restart_and_resumes(self):
        batch = self.opened()
        batch_id = batch["batch_id"]
        self.lake.append_segment(batch_id, 1, [self.visit("a")])
        del self.lake
        revived = ExhibitLake(self.path)
        unfinished = revived.unfinished_batches("visits")["unfinished"]
        self.assertEqual([x["batch_id"] for x in unfinished], [batch_id])
        self.assertEqual(unfinished[0]["segments"], 1)
        revived.append_segment(batch_id, 2, [self.visit("b")])
        sealed = revived.seal_batch(batch_id)
        self.assertEqual(sealed["valid_rows"], 2)
        self.assertEqual(revived.unfinished_batches("visits")["unfinished"], [])

    def test_unfinished_batch_can_be_abandoned_after_restart(self):
        batch = self.opened("lost")
        batch_id = batch["batch_id"]
        self.lake.append_segment(batch_id, 1, [self.visit("a")])
        del self.lake
        revived = ExhibitLake(self.path)
        result = revived.abandon_batch(batch_id, "operator gave up")
        self.assertEqual(result["status"], "abandoned")
        self.assertEqual(result["abandon_reason"], "operator gave up")
        self.assertEqual(revived.unfinished_batches()["unfinished"], [])
        self.assertEqual(revived.snapshots.inventory("venue", "2026-03-01")["sources"], [])
        with self.assertRaises(StateError):
            revived.append_segment(batch_id, 2, [self.visit("b")])
        with self.assertRaises(StateError):
            revived.seal_batch(batch_id)

    def test_abandoned_batch_cannot_be_reopened(self):
        batch = self.opened("lost")
        self.lake.abandon_batch(batch["batch_id"])
        with self.assertRaises(StateError):
            self.opened("lost")

    def test_sealed_batch_cannot_be_abandoned(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        self.lake.seal_batch(batch["batch_id"])
        with self.assertRaises(StateError):
            self.lake.abandon_batch(batch["batch_id"])

    # ------------------------------------------------------------------
    # integrity and traceability
    # ------------------------------------------------------------------
    def test_declared_totals_guard_seal(self):
        batch = self.opened(expected_rows=3)
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        with self.assertRaises(Conflict):
            self.lake.seal_batch(batch["batch_id"])
        self.assertEqual(self.published_events(), [])
        # The stream remains open after a failed seal, so more rows can be added.
        self.lake.append_segment(batch["batch_id"], 2, [self.visit("b"), self.visit("c")])
        sealed = self.lake.seal_batch(batch["batch_id"])
        self.assertEqual(sealed["valid_rows"], 3)

    def test_declared_payload_hash_guards_seal(self):
        from exhibit_lake.ingest.validation import digest
        rows = [self.visit("a"), self.visit("b")]
        batch = self.opened(expected_payload_hash=digest(rows))
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        self.lake.append_segment(batch["batch_id"], 2, [self.visit("b")])
        self.assertEqual(self.lake.seal_batch(batch["batch_id"])["valid_rows"], 2)
        wrong = self.opened("other", expected_payload_hash=digest([self.visit("z")]))
        self.lake.append_segment(wrong["batch_id"], 1, [self.visit("a")])
        with self.assertRaises(Conflict):
            self.lake.seal_batch(wrong["batch_id"])

    def test_whole_batch_hash_matches_single_shot_semantics(self):
        rows = [self.visit("a"), self.visit("b", 4)]
        whole = self.lake.open_batch("visits", "v1", "one", expected_payload_hash=None)
        self.lake.append_segment(whole["batch_id"], 1, rows[:1])
        self.lake.append_segment(whole["batch_id"], 2, rows[1:])
        self.lake.seal_batch(whole["batch_id"])
        direct = self.lake.ingest("visits", "v1", "two", rows)
        self.assertEqual(
            self.lake.batches.report(whole["batch_id"])["status"], direct["status"])
        with self.lake.store.transaction(write=False) as conn:
            hashes = [r["payload_hash"] for r in conn.execute(
                "SELECT payload_hash FROM batches WHERE batch_id IN (?,?)", (whole["batch_id"], direct["batch_id"]))]
        self.assertEqual(len(set(hashes)), 1)

    def test_unsealed_batch_integrity_is_valid_and_counts_pending(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a"), self.visit("bad", 0)])
        report = self.lake.audit.batch_integrity(batch["batch_id"])
        self.assertTrue(report["valid"], report)
        self.assertEqual((report["raw_rows"], report["pending_rows"]), (2, 2))
        self.assertFalse(report["sealed"])

    def test_raw_rows_trace_final_disposition(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a"), self.visit("bad", 0)])
        pending = self.lake.batches.raw(batch["batch_id"])["rows"]
        self.assertEqual([x["disposition"]["outcome"] for x in pending], ["pending", "pending"])
        self.assertEqual(pending[0]["disposition"]["event_id"], "a")
        self.assertEqual(pending[1]["disposition"]["code"], "invalid_count")
        self.lake.seal_batch(batch["batch_id"])
        rows = self.lake.batches.raw(batch["batch_id"])["rows"]
        self.assertEqual(rows[0]["disposition"], {"outcome": "accepted", "event_id": "a"})
        self.assertEqual(rows[1]["disposition"], {"outcome": "rejected", "code": "invalid_count"})
        self.assertEqual(self.lake.provenance.event("visits", "a")["raw"], self.visit("a"))

    def test_abandoned_raw_rows_trace_as_abandoned(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("a")])
        self.lake.abandon_batch(batch["batch_id"])
        self.assertEqual(self.lake.batches.raw(batch["batch_id"])["rows"][0]["disposition"],
                         {"outcome": "abandoned", "event_id": "a"})

    def test_duplicate_identity_across_segments_isolated_at_seal(self):
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, [self.visit("same", 1)])
        self.lake.append_segment(batch["batch_id"], 2, [self.visit("same", 1)])
        sealed = self.lake.seal_batch(batch["batch_id"])
        self.assertEqual((sealed["valid_rows"], sealed["rejected_rows"]), (1, 1))
        self.assertEqual(sealed["rejections"][0]["row_no"], 2)
        self.assertEqual(sealed["rejections"][0]["code"], "duplicate_event")

    def test_validation_failures(self):
        batch = self.opened()
        with self.assertRaises(InvalidInput):
            self.lake.append_segment(batch["batch_id"], 0, [self.visit()])
        with self.assertRaises(InvalidInput):
            self.lake.append_segment(batch["batch_id"], 1, [])
        with self.assertRaises(InvalidInput):
            self.lake.append_segment(batch["batch_id"], 1, [self.visit(count=float("nan"))])
        with self.assertRaises(NotFound):
            self.lake.append_segment("missing", 1, [self.visit()])
        empty = self.opened("empty")
        with self.assertRaises(StateError):
            self.lake.seal_batch(empty["batch_id"])

    def test_failed_segment_lands_nothing(self):
        batch = self.opened()
        with self.assertRaises(InvalidInput):
            self.lake.append_segment(batch["batch_id"], 1, [self.visit("a"), self.visit("b", float("inf"))])
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["landed_rows"], 0)
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["segments"], [])

    def test_open_conflicts_with_batch_lacking_stream_record(self):
        batch_id = self.lake.ingest("visits", "v1", "batch", [self.visit("a")])["batch_id"]
        # Simulate a batch landed by an older build without segmentation metadata.
        with self.lake.store.transaction() as conn:
            conn.execute("DELETE FROM batch_streams WHERE batch_id=?", (batch_id,))
        with self.assertRaises(Conflict):
            self.opened()

    def test_whole_batch_resend_recovers_interrupted_stream(self):
        rows = [self.visit("a"), self.visit("b", 3), self.visit("c", 2)]
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, rows[:2])
        self.lake.append_segment(batch["batch_id"], 2, rows[2:])
        # Crash between the last segment and seal: the client retries the whole batch.
        recovered = self.lake.ingest("visits", "v1", "batch", rows)
        self.assertEqual(recovered["batch_id"], batch["batch_id"])
        self.assertFalse(recovered["replay"])
        self.assertEqual(recovered["valid_rows"], 3)
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["landed_rows"], 3)
        self.assertEqual(self.lake.unfinished_batches()["unfinished"], [])

    def test_partial_resend_of_unfinished_stream_conflicts(self):
        rows = [self.visit("a"), self.visit("b"), self.visit("c")]
        batch = self.opened()
        self.lake.append_segment(batch["batch_id"], 1, rows[:1])
        with self.assertRaises(Conflict):
            self.lake.ingest("visits", "v1", "batch", rows)
        self.assertEqual(self.lake.batch_pending(batch["batch_id"])["landed_rows"], 1)

    def test_unfinished_filter_by_source(self):
        self.lake.register_source("other", "venue", "visit")
        self.lake.register_contract("o1", "other", ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")
        mine = self.opened()["batch_id"]
        other = self.lake.open_batch("other", "o1", "key")["batch_id"]
        self.assertEqual([x["batch_id"] for x in self.lake.unfinished_batches()["unfinished"]], [mine, other])
        filtered = self.lake.unfinished_batches("visits")["unfinished"]
        self.assertEqual([x["batch_id"] for x in filtered], [mine])
        with self.assertRaises(NotFound):
            self.lake.unfinished_batches("missing")

    def test_seal_validates_declared_hash_before_writing_rows(self):
        wrong = self.opened("wrong", expected_payload_hash="0" * 64)
        self.lake.append_segment(wrong["batch_id"], 1, [self.visit("a")])
        with self.assertRaises(Conflict):
            self.lake.seal_batch(wrong["batch_id"])
        report = self.lake.audit.batch_integrity(wrong["batch_id"])
        self.assertTrue(report["valid"], report)
        self.assertEqual(report["accepted_rows"], 0)


class SegmentedIngestApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.db")))
        self.client.post("/venues", json={"venue_id": "venue", "name": "Venue", "timezone": "Asia/Shanghai"})
        self.client.post("/exhibits", json={"exhibit_id": "robot", "venue_id": "venue", "name": "Robot",
                                            "maker": "Maker", "launched_on": "2026-01-01"})
        self.client.post("/sources", json={"source_id": "counter", "venue_id": "venue", "kind": "visit"})
        self.client.post("/contracts", json={"contract_id": "v1", "source_id": "counter",
                                             "fields": ["event_id", "exhibit_id", "event_at", "count"],
                                             "effective_from": "2026-01-01T00:00:00+00:00"})

    def row(self, event_id="v1", count=5):
        return {"event_id": event_id, "exhibit_id": "robot", "event_at": STAMP, "count": count}

    def test_segmented_flow_over_http(self):
        opened = self.client.post("/batches/open", json={"source_id": "counter", "contract_id": "v1",
                                                         "request_key": "key"})
        self.assertEqual(opened.status_code, 201, opened.text)
        batch_id = opened.json()["batch_id"]
        segment = self.client.post(f"/batches/{batch_id}/segments",
                                   json={"segment_no": 1, "events": [self.row()]})
        self.assertEqual(segment.status_code, 201, segment.text)
        self.assertEqual(self.client.get("/partitions/venue/2026-03-01").json()["inventory"]["sources"], [])
        replay = self.client.post(f"/batches/{batch_id}/segments",
                                  json={"segment_no": 1, "events": [self.row()]})
        self.assertTrue(replay.json()["replay"])
        divergent = self.client.post(f"/batches/{batch_id}/segments",
                                     json={"segment_no": 1, "events": [self.row(count=1)]})
        self.assertEqual(divergent.status_code, 409)
        sealed = self.client.post(f"/batches/{batch_id}/seal")
        self.assertEqual(sealed.status_code, 200, sealed.text)
        self.assertEqual(sealed.json()["valid_rows"], 1)
        report = self.client.get(f"/batches/{batch_id}/pending").json()
        self.assertTrue(report["sealed"])
        self.assertEqual(self.client.get("/batches/unfinished").json()["unfinished"], [])

    def test_abandon_over_http(self):
        batch_id = self.client.post("/batches/open", json={
            "source_id": "counter", "contract_id": "v1", "request_key": "key"}).json()["batch_id"]
        self.client.post(f"/batches/{batch_id}/segments", json={"segment_no": 1, "events": [self.row()]})
        abandoned = self.client.post(f"/batches/{batch_id}/abandon", json={"reason": "operator"})
        self.assertEqual(abandoned.status_code, 200)
        self.assertEqual(abandoned.json()["status"], "abandoned")
        self.assertEqual(self.client.post(f"/batches/{batch_id}/seal").status_code, 409)

    def test_legacy_batch_endpoint_still_works(self):
        result = self.client.post("/batches", json={"source_id": "counter", "contract_id": "v1",
                                                    "request_key": "legacy", "events": [self.row()]})
        self.assertEqual(result.status_code, 201)
        self.assertEqual(result.json()["valid_rows"], 1)
        self.assertEqual(self.client.get(f"/batches/{result.json()['batch_id']}/integrity").json()["valid"], True)


if __name__ == "__main__":
    unittest.main()
