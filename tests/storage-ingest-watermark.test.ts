/**
 * #3133: per-session ingest watermark — O(new messages) per save.
 *
 * The full-log ingest cost (JSON.stringify + sha256 per message + a scan of
 * every existing row) made each shadow/full save O(full log): ~25-40ms per
 * MB, ~0.6s for a 14k-message session, synchronous on the save path. The
 * watermark names, on the sessions row, the abs seq backing the LAST tail
 * message of the most recent save; the trusted-prefix length is re-read from
 * tail_json's tailSeqs (same transaction wrote both). A save whose tail still
 * ends with that message at that slot appends ONLY the new messages
 * (stringify+hash+INSERT each), anchored at max(seq)+1 — folds leave rows
 * above the trusted tip and the anchor absorbs them. Every doubt (NULL
 * watermark, tampered/missing boundary row, rewound tail, snapshot-bearing
 * session) falls back to the byte-identical full ingest, so a wrong
 * watermark can only ever cost one redundant full write, never wrong rows.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createHash } from "node:crypto";
import { UnifiedStore } from "../src/storage/store.ts";
import { UnifiedShadow } from "../src/storage/shadow.ts";
import { openSqliteDatabase } from "../src/storage/driver.ts";
import { planIngest, planIngestDelta, loadLegacyView, ingestSerializations, type LegacySessionLike } from "../src/storage/ingest.ts";
import type { CoreMessage } from "acp-kernel";

process.env.BILI_PERSIST_ZSTD = "0";
// Same hermeticity pin as the other storage suites: a developer machine with
// storage.unified in its real config must not leak a tier into these tests
// (the shadow here is constructed directly with its own env object anyway).
const hermetic = fs.mkdtempSync(path.join(os.tmpdir(), "bili-watermark-hermetic-"));
process.env.BILI_CONFIG_FILE = path.join(hermetic, "no-config.json");
process.env.BILI_STORAGE_UNIFIED = "legacy";

function tmpdir(prefix: string): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function rmBestEffort(p: string): void {
    try {
        fs.rmSync(p, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch {
        /* leaked temp dir on a Windows runner is harmless */
    }
}

function msg(i: number, text = `hello ${i}`): CoreMessage {
    return { id: `h_msg${i}`, role: i % 2 === 0 ? "user" : "assistant", contentType: "text", text } as CoreMessage;
}

function mkLegacy(id: string, lastMessages: LegacySessionLike["lastMessages"], pluginSnapshot?: LegacySessionLike["pluginSnapshot"]): LegacySessionLike {
    return {
        id,
        createdAt: 1_000,
        lastSeen: 2_000,
        meta: { protocol: "anthropic" },
        stats: { requests: 1 },
        metadata: {},
        // Key order matters: loadLegacyView rebuilds state in MERGE_STATE_FIELDS
        // order and the shadow digest is JSON.stringify-based (order-sensitive).
        state: {
            blocks: [],
            messageRefs: { byRaw: {}, byRef: {} },
            nextBlockId: 1,
            nextRunId: 1,
        },
        blockContents: new Map(),
        lastMessages,
        lastMessagesFolded: false,
        pluginSnapshot,
    };
}

interface StatsView {
    ingests: number;
    deltaIngests: number;
    verifySamples: number;
    verifyOk: number;
    verifyDrift: number;
}

function openShadow(dir: string): { shadow: UnifiedShadow; store: UnifiedStore } {
    const store = UnifiedStore.open(path.join(dir, "index.db"));
    const shadow = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, (r) => r as LegacySessionLike);
    assert.ok(shadow, "shadow mode sidecar must open");
    return { shadow, store };
}

function stats(shadow: UnifiedShadow): StatsView {
    return (shadow as unknown as { stats: StatsView }).stats;
}

function view(store: UnifiedStore, id: string): ReturnType<typeof loadLegacyView> {
    const session = store.getSessionRow(id);
    assert.ok(session, `session row must exist: ${id}`);
    return loadLegacyView({
        session,
        messages: store.getMessages(id),
        blocks: store.getBlocks(id),
        deadRefs: store.getDeadRefs(id),
        refs: store.getRefs(id),
    });
}

/** The equivalence oracle: whatever the delta path wrote must equal what a
 *  full re-ingest of the same legacy view would write (deep view equality —
 *  state, tail, pointers). */
function assertDeltaMatchesFull(store: UnifiedStore, legacy: LegacySessionLike): void {
    const before = view(store, legacy.id);
    assert.equal(store.ingestSession(planIngest(legacy)), true, "full re-ingest must apply");
    assert.deepEqual(view(store, legacy.id), before, "delta-written rows must equal a full re-ingest's");
}

test("watermark: steady-state saves cost O(new messages), not O(full log)", () => {
    const dir = tmpdir("bili-wm-steady-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const n = 12;
            let legacy = mkLegacy("wm-steady", Array.from({ length: n }, (_, i) => msg(i)));

            ingestSerializations.count = 0;
            shadow.record(legacy);
            assert.equal(stats(shadow).deltaIngests, 0, "first save has no watermark yet — full ingest");
            assert.equal(ingestSerializations.count, n, "full ingest serializes every message once");
            assert.deepEqual(store.ingestWatermark("wm-steady"), { seq: n - 1, tailSeqs: Array.from({ length: n }, (_, i) => i) });

            // +1 message: probe(1) + new(1) — 2 serializations, not 13.
            legacy = mkLegacy("wm-steady", Array.from({ length: n + 1 }, (_, i) => msg(i)));
            ingestSerializations.count = 0;
            shadow.record(legacy);
            assert.equal(stats(shadow).deltaIngests, 1, "second save must take the delta path");
            assert.equal(ingestSerializations.count, 2, "delta serializes only the boundary probe + the new message");
            assert.equal(store.getMessages("wm-steady").length, n + 1, "row count tracks the tail");
            assertDeltaMatchesFull(store, legacy);

            // +3 messages: probe(1) + new(3) — cost scales with NEW, not total.
            legacy = mkLegacy("wm-steady", Array.from({ length: n + 4 }, (_, i) => msg(i)));
            ingestSerializations.count = 0;
            shadow.record(legacy);
            assert.equal(stats(shadow).deltaIngests, 2);
            assert.equal(ingestSerializations.count, 4);
            const mark = store.ingestWatermark("wm-steady");
            assert.ok(mark, "watermark re-established after each delta");
            assert.equal(mark.seq, n + 3, "watermark names the last tail message's seq");
            assertDeltaMatchesFull(store, legacy);
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: zero-new-message save stays on the delta path", () => {
    const dir = tmpdir("bili-wm-idle-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const n = 10;
            let legacy = mkLegacy("wm-idle", Array.from({ length: n }, (_, i) => msg(i)));
            shadow.record(legacy);
            assert.equal(stats(shadow).deltaIngests, 0, "first save is a full ingest");

            // Same tail, changed metadata only (stats bump): the delta path
            // must commit an EMPTY delta — sessions-row write, zero message
            // serializations, not a full re-ingest.
            const tailNow = legacy.lastMessages ?? [];
            legacy = mkLegacy("wm-idle", tailNow);
            legacy.stats = { requests: 2 };
            ingestSerializations.count = 0;
            // Production metadata-only saves carry the raw tail (persist
            // passes it whenever lastMessages is non-empty).
            shadow.record(legacy, { lastMessages: tailNow });
            assert.equal(stats(shadow).deltaIngests, 1, "metadata-only save must not fall back to full");
            assert.equal(ingestSerializations.count, 1, "only the boundary probe serializes");
            assert.equal(store.getMessages("wm-idle").length, n, "no message rows change");
            assert.deepEqual(store.ingestWatermark("wm-idle"), { seq: n - 1, tailSeqs: Array.from({ length: n }, (_, i) => i) });
            assertDeltaMatchesFull(store, legacy);
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: envelope re-render without rawTail never rides the delta (#2960 ⑤ order)", () => {
    const dir = tmpdir("bili-wm-render-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            // Raw-tail lineage: summary carrier first, then raw messages.
            const msgs = [msg(0), msg(1), msg(2)];
            const carrier = msg(99);
            carrier.id = "acp_summary_b1";
            const lineage = [carrier, ...msgs];
            let legacy = mkLegacy("wm-render", lineage);
            shadow.record(legacy, { lastMessages: lineage });
            assert.equal(stats(shadow).deltaIngests, 0, "first save is a full ingest");
            const rawOrder = store.getMessages("wm-render").map((r) => r.msgId);
            assert.deepEqual(rawOrder, ["acp_summary_b1", "h_msg0", "h_msg1", "h_msg2"]);

            // Stale-convergence path shape: record(disk) with NO rawTail, the
            // envelope's folded re-render presenting the SAME identities in a
            // DIFFERENT order and the same length. The #7474 reanchor (KDD #23,
            // "OFF 臂同受益") now rides here — byte-equivalent to the full path
            // (same served view, same watermark) — so the served view must
            // follow the envelope render order, not the stale raw ordering.
            const envRender = [msgs[0], carrier, msgs[1], msgs[2]];
            legacy = mkLegacy("wm-render", envRender);
            shadow.record(legacy);
            assert.equal(stats(shadow).deltaIngests, 1, "envelope render rides the reanchor delta (#7474 KDD #23)");
            const served = view(store, "wm-render");
            assert.ok(served.lastMessages);
            assert.deepEqual(served.lastMessages.map((m) => m.id), ["h_msg0", "acp_summary_b1", "h_msg1", "h_msg2"], "served view must follow the envelope render order");
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: delta survives a fold (holes above the trusted tip)", () => {
    const dir = tmpdir("bili-wm-fold-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const tail0 = Array.from({ length: 8 }, (_, i) => msg(i));
            shadow.record(mkLegacy("wm-fold", tail0));
            assert.equal(stats(shadow).deltaIngests, 0);

            // Fold: m2..m5 collapse into a new summary message; the tail
            // SHRINKS below the trusted length — the #7474 reanchor (KDD #23)
            // rides (byte-equivalent to the full path), and the summary lands
            // ABOVE the surviving old rows (abs 8).
            const summary = { id: "h_summary", role: "user", contentType: "text", text: "summary of 2..5" } as CoreMessage;
            const tail1 = [tail0[0], tail0[1], summary, tail0[6], tail0[7]];
            shadow.record(mkLegacy("wm-fold", tail1));
            assert.equal(stats(shadow).deltaIngests, 1, "fold rides the reanchor delta (#7474 KDD #23)");
            const mark1 = store.ingestWatermark("wm-fold");
            assert.ok(mark1, "reanchor re-establishes the watermark at the fold (#7474 KDD #23)");
            assert.deepEqual(mark1.tailSeqs, [0, 1, 8, 6, 7], "tail pointers carry the holes verbatim");
            assert.equal(mark1.seq, 7, "watermark = seq of the LAST tail message, not the max");

            // Post-fold append: delta must work IMMEDIATELY (anchored at
            // max(seq)+1 = 9, skipping the summary's slot is fine).
            const tail2 = [...tail1, msg(8), msg(9)];
            ingestSerializations.count = 0;
            shadow.record(mkLegacy("wm-fold", tail2));
            assert.equal(stats(shadow).deltaIngests, 2, "post-fold append takes the delta path (fold + append)");
            assert.equal(ingestSerializations.count, 3, "probe + 2 new messages");
            const mark2 = store.ingestWatermark("wm-fold");
            assert.ok(mark2);
            assert.deepEqual(mark2.tailSeqs, [0, 1, 8, 6, 7, 9, 10], "appends continue after the max, not the watermark");
            assert.deepEqual(view(store, "wm-fold").lastMessages, tail2, "the read view is the exact folded tail");
            assertDeltaMatchesFull(store, mkLegacy("wm-fold", tail2));
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: boundary tamper falls back to full ingest and re-establishes", () => {
    const dir = tmpdir("bili-wm-tamper-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const n = 10;
            const at = (k: number) => mkLegacy("wm-tamper", Array.from({ length: k }, (_, i) => msg(i)));
            shadow.record(at(n));
            assert.equal(stats(shadow).deltaIngests, 0);
            const wm = store.ingestWatermark("wm-tamper");
            assert.ok(wm && wm.seq === n - 1);

            // Tamper leg 1: foreign UPDATE of the boundary row's hash (the
            // junk content row exists purely to satisfy the FK).
            const conn = openSqliteDatabase(path.join(dir, "index.db")).db;
            try {
                const junkBytes = Buffer.from(JSON.stringify({ id: "h_tampered", role: "user", contentType: "text", text: "tampered" }), "utf8");
                conn.prepare("INSERT OR IGNORE INTO content (hash, byte_len, refcount, bytes) VALUES ('deadbeef', ?, 1, ?)").run(junkBytes.length, junkBytes);
                conn.prepare("UPDATE messages SET content_hash = 'deadbeef' WHERE session_id = ? AND seq = ?").run("wm-tamper", wm.seq);
            } finally {
                conn.close();
            }
            shadow.record(at(n + 1));
            assert.equal(stats(shadow).deltaIngests, 0, "tampered boundary must force the full path");
            // The tampered row survives as an orphan at its old seq (its
            // identity no longer matches the tail's message), and the true
            // message re-enters at nextSeq — so assert on the VIEW (the
            // tailSeqs-referenced messages), not the physical row count.
            assert.deepEqual(view(store, "wm-tamper").lastMessages, at(n + 1).lastMessages, "full fallback writes every tail message correctly");
            const wm2 = store.ingestWatermark("wm-tamper");
            assert.ok(wm2, "watermark re-established after the full re-ingest");
            assertDeltaMatchesFull(store, at(n + 1));

            // Tamper leg 2: foreign DELETE of the boundary row.
            const conn2 = openSqliteDatabase(path.join(dir, "index.db")).db;
            try {
                conn2.prepare("DELETE FROM messages WHERE session_id = ? AND seq = ?").run("wm-tamper", wm2.seq);
            } finally {
                conn2.close();
            }
            shadow.record(at(n + 2));
            assert.equal(stats(shadow).deltaIngests, 0, "missing boundary row must force the full path");
            assert.deepEqual(view(store, "wm-tamper").lastMessages, at(n + 2).lastMessages);
            assertDeltaMatchesFull(store, at(n + 2));

            // Recovery: with the boundary intact again, the next save deltas.
            shadow.record(at(n + 3));
            assert.equal(stats(shadow).deltaIngests, 1, "delta path resumes once the watermark is trustworthy");
            assertDeltaMatchesFull(store, at(n + 3));
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: foreign rows above the watermark do not corrupt the delta", () => {
    const dir = tmpdir("bili-wm-foreign-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const n = 6;
            const at = (k: number) => mkLegacy("wm-foreign", Array.from({ length: k }, (_, i) => msg(i)));
            shadow.record(at(n));
            const wm = store.ingestWatermark("wm-foreign");
            assert.ok(wm);

            // A non-bili writer drops a junk row above the trusted tip. The
            // boundary identity is intact, so the delta still runs — but its
            // appends anchor at max(seq)+1, never colliding with the junk.
            const conn = openSqliteDatabase(path.join(dir, "index.db")).db;
            try {
                const junk = Buffer.from(JSON.stringify({ id: "h_junk", role: "user", contentType: "text", text: "foreign" }), "utf8");
                conn.prepare("INSERT INTO content (hash, byte_len, refcount, bytes) VALUES ('junkhash', ?, 1, ?)").run(junk.length, junk);
                conn
                    .prepare("INSERT INTO messages (session_id, seq, msg_id, ref, ref_num, role, kind, content_hash) VALUES (?, ?, ?, NULL, NULL, 'user', 'message', 'junkhash')")
                    .run("wm-foreign", wm.seq + 5, "h_junk");
            } finally {
                conn.close();
            }
            shadow.record(at(n + 1));
            assert.equal(stats(shadow).deltaIngests, 1, "intact boundary still deltas");
            const mark = store.ingestWatermark("wm-foreign");
            assert.ok(mark);
            assert.equal(mark.seq, wm.seq + 6, "append lands above the foreign row, watermark advances to it");
            const rows = store.getMessages("wm-foreign");
            assert.equal(rows.length, n + 2, "tail rows + the junk row, nothing lost");
            assert.deepEqual(view(store, "wm-foreign").lastMessages, at(n + 1).lastMessages, "the read view never references the junk");
            assertDeltaMatchesFull(store, at(n + 1));
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: fresh session (NULL watermark) runs the full ingest", () => {
    const dir = tmpdir("bili-wm-fresh-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            assert.equal(store.ingestWatermark("never-seen"), null, "no sessions row → NULL watermark");
            shadow.record(mkLegacy("wm-fresh", [msg(0), msg(1), msg(2)]));
            assert.equal(stats(shadow).deltaIngests, 0);
            assert.deepEqual(store.ingestWatermark("wm-fresh"), { seq: 2, tailSeqs: [0, 1, 2] });
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: remove() clears it; importRecord re-establishes it", () => {
    const dir = tmpdir("bili-wm-remove-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const at = (k: number) => mkLegacy("wm-remove", Array.from({ length: k }, (_, i) => msg(i)));
            shadow.record(at(5));
            assert.equal(stats(shadow).deltaIngests, 0);

            shadow.remove("wm-remove");
            assert.equal(store.getSessionRow("wm-remove"), null, "deleteSession drops the row (watermark dies with it)");
            assert.equal(store.ingestWatermark("wm-remove"), null);

            shadow.record(at(6));
            assert.equal(stats(shadow).deltaIngests, 0, "post-remove save must re-ingest fully");
            assert.equal(store.getMessages("wm-remove").length, 6);

            // Re-import (organic full-mode import path): full ingest by
            // construction, watermark re-established for later deltas.
            shadow.remove("wm-remove");
            const imported = shadow.importRecord(at(6));
            assert.ok(imported, "importRecord returns the legacy view");
            assert.deepEqual(store.ingestWatermark("wm-remove"), { seq: 5, tailSeqs: [0, 1, 2, 3, 4, 5] });
            shadow.record(at(7));
            assert.equal(stats(shadow).deltaIngests, 1, "saves delta again after the re-import");
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: rewound/re-derived tail falls back to full (no silent drops)", () => {
    const dir = tmpdir("bili-wm-rewind-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            // Save [m0..m4]; then the client rewinds and RE-DERIVES m1/m2
            // byte-identically (same ids + content) appended after m4 — the
            // identities already have rows at seqs 1/2.
            const tail0 = Array.from({ length: 5 }, (_, i) => msg(i));
            shadow.record(mkLegacy("wm-rewind", tail0));
            const tail1 = [...tail0, msg(1), msg(2)];
            shadow.record(mkLegacy("wm-rewind", tail1));
            assert.equal(stats(shadow).deltaIngests, 0, "re-derived identities must not ride the delta");
            const v = view(store, "wm-rewind");
            assert.deepEqual(v.lastMessages, tail1, "the full path points the re-derived slots at their existing rows");
            assert.equal(store.getMessages("wm-rewind").length, 5, "no duplicate rows for re-derived identities");
            assertDeltaMatchesFull(store, mkLegacy("wm-rewind", tail1));
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: every duplicate-identity tail shape falls back to full — corpus from #4182", () => {
    const dir = tmpdir("bili-wm-resend-corpus-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            // The #4182 corpus: every shape whose NEW tail portion carries an
            // already-stored identity (the delta planner cannot see stored
            // identities, so its raw plan diverges from planIngest) must be
            // refused by the store's strictly-append guard and fall back to
            // the exact full path — final state equal to a pure full ingest,
            // no extra rows. The no-duplicate control must still ride the
            // delta (the guard must not over-refuse).
            const shapes: { name: string; first: CoreMessage[]; second: CoreMessage[]; ridesDelta: boolean }[] = [
                { name: "adjacent duplicate at the tip", first: [msg(0), msg(1), msg(2)], second: [msg(0), msg(1), msg(2), msg(2)], ridesDelta: false },
                { name: "duplicate in the middle of the new tail", first: [msg(0), msg(1), msg(2)], second: [msg(0), msg(1), msg(2), msg(0), msg(2)], ridesDelta: false },
                { name: "double duplicate (both prefix messages resent)", first: [msg(0), msg(1)], second: [msg(0), msg(1), msg(0), msg(1)], ridesDelta: false },
                { name: "duplicate of an interior prefix message", first: [msg(0), msg(1), msg(2), msg(3)], second: [msg(0), msg(1), msg(2), msg(3), msg(1)], ridesDelta: false },
                { name: "control: no duplicate", first: [msg(0), msg(1), msg(2)], second: [msg(0), msg(1), msg(2), msg(4)], ridesDelta: true },
            ];
            shapes.forEach((shape, k) => {
                const id = `wm-corpus-${k}`;
                const before = stats(shadow).deltaIngests;
                shadow.record(mkLegacy(id, shape.first));
                shadow.record(mkLegacy(id, shape.second));
                assert.equal(stats(shadow).deltaIngests - before, shape.ridesDelta ? 1 : 0, `${shape.name}: ${shape.ridesDelta ? "must ride" : "must NOT ride"} the delta`);
                // Oracle: what a pure full ingest of the same view writes.
                const fullPlan = planIngest(mkLegacy(id, shape.second));
                const expTail = (JSON.parse(fullPlan.session.tailJson) as { tailSeqs: number[] }).tailSeqs;
                assert.equal(store.getMessages(id).length, fullPlan.messages.length, `${shape.name}: no extra rows for resent identities`);
                assert.deepEqual(store.ingestWatermark(id), { seq: expTail[expTail.length - 1], tailSeqs: expTail }, `${shape.name}: watermark/tailSeqs equal the full ingest's`);
                assert.deepEqual(view(store, id).lastMessages, shape.second, `${shape.name}: served view keeps every tail slot`);
            });
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("planner contract (#4182): duplicate-identity tail plans diverge BY DESIGN; the store guard compensates", () => {
    const A = msg(0);
    const B = msg(1);
    const hB = createHash("sha256").update(JSON.stringify(B)).digest("hex");
    const full = planIngest(mkLegacy("p-full", [A, B, A]));
    assert.equal(full.messages.length, 2, "full plan dedups the resent identity onto its existing seq");
    assert.deepEqual((JSON.parse(full.session.tailJson) as { tailSeqs: number[] }).tailSeqs, [0, 1, 0]);
    const delta = planIngestDelta(mkLegacy("p-delta", [A, B, A]), 1, [0, 1], { msgId: B.id, contentHash: hB }, true);
    assert.ok(delta, "the delta planner accepts the shape — it cannot see stored prefix identities");
    assert.equal(delta!.messages.length, 1, "only the resent tail slot is emitted as a new row");
    assert.equal(delta!.messages[0].msgId, A.id);
    // THE #4182 divergence, pinned: the resend gets a FRESH seq here where
    // planIngest above pointed the same slot at A's existing seq (…0 vs …2).
    // Compensated at the store layer by the strictly-append guard.
    assert.deepEqual((JSON.parse(delta!.session.tailJson) as { tailSeqs: number[] }).tailSeqs, [0, 1, 2]);
});

test("watermark: snapshot-bearing sessions stay on the full path", () => {
    const dir = tmpdir("bili-wm-snapshot-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            const snapshot = [msg(0), msg(1)];
            const tail = [msg(0), msg(1), msg(2)];
            shadow.record(mkLegacy("wm-snap", tail, snapshot));
            // The full path sets the watermark here too (the fact "seq W backs
            // the last tail message" holds regardless of snapshot shape), but
            // it stays INERT: the delta planner refuses snapshot-bearing
            // sessions, so their saves never ride it.
            assert.ok(store.ingestWatermark("wm-snap"), "watermark fact is recorded");
            shadow.record(mkLegacy("wm-snap", [...tail, msg(3)], snapshot));
            assert.equal(stats(shadow).deltaIngests, 0, "pluginSnapshot present → always full");
            // The pure planner gate agrees (defense in depth).
            assert.equal(planIngestDelta(mkLegacy("wm-snap", [...tail, msg(4)], snapshot), 2, [0, 1, 2], { msgId: "h_msg2", contentHash: "x" }, true), null);
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: sampled verification still catches drift through the delta path", async () => {
    const dir = tmpdir("bili-wm-verify-");
    try {
        const { shadow, store } = openShadow(dir);
        try {
            // 21 appends: save 1 full (watermark set), saves 2..21 all delta.
            let k = 5;
            shadow.record(mkLegacy("wm-verify", Array.from({ length: k }, (_, i) => msg(i))));
            const s0 = stats(shadow);
            assert.equal(s0.verifySamples, 1, "first ingest verifies");

            // Tamper the CAS body under a tail row the delta path will NOT
            // rewrite (the trusted prefix) — only a sampled read-back can
            // catch it, exactly as pre-#3133.
            const conn = openSqliteDatabase(path.join(dir, "index.db")).db;
            try {
                const row = conn.prepare("SELECT content_hash FROM messages WHERE session_id = ? ORDER BY seq LIMIT 1").get("wm-verify") as { content_hash: string } | undefined;
                assert.ok(row, "row to tamper must exist");
                conn
                    .prepare("UPDATE content SET bytes = ? WHERE hash = ?")
                    .run(Buffer.from(JSON.stringify({ id: "tampered", role: "user", contentType: "text", text: "tampered" }), "utf8"), row.content_hash);
            } finally {
                conn.close();
            }

            for (let i = 0; i < 20; i++) {
                k += 1;
                shadow.record(mkLegacy("wm-verify", Array.from({ length: k }, (_, j) => msg(j))));
            }
            const s1 = stats(shadow);
            assert.equal(s1.deltaIngests, 20, "every post-watermark save took the delta path");
            assert.equal(s1.verifySamples, 2, "verify runs on ingest 1 and 21 only");
            assert.equal(s1.verifyOk, 1, "only the pre-tamper sample may pass");
            assert.equal(s1.verifyDrift, 1, "the sampled save at n=21 must catch the CAS drift");
            assert.equal(store.getMessages("wm-verify").length, k, "rows kept growing through the deltas");
            // #3166 review: the drift backstop must HEAL, not only report —
            // the watermark still vouches for a prefix the digest just proved
            // stale, so it is dropped and the NEXT save re-points every tail
            // slot through the exact full path. No byte-parity assertion
            // here: the tamper corrupted the CAS BYTES under a known hash,
            // and content addressing means a re-ingest of the same logical
            // message legitimately lands on those bytes again (CAS repair is
            // a separate integrity lane). The heal contract pinned here is
            // pointer-level: watermark gone, next save full, row appended.
            assert.equal(store.ingestWatermark("wm-verify"), null, "drift must clear the durable watermark");
            k += 1;
            shadow.record(mkLegacy("wm-verify", Array.from({ length: k }, (_, j) => msg(j))));
            const s2 = stats(shadow);
            assert.equal(s2.deltaIngests, 20, "post-drift save takes the full path");
            assert.equal(store.getMessages("wm-verify").length, k, "full re-ingest appended the new row");
        } finally {
            shadow.close();
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: v4 database migrates to v12 additively; newer user_version no-ops", async () => {
    const dir = tmpdir("bili-wm-migrate-");
    try {
        const dbPath = path.join(dir, "index.db");
        // Craft the v4 shape: the current fresh-install DDL minus the v5
        // watermark column, one live session with message rows,
        // user_version=4. Newer additive shapes still in the DDL (v6 orphans
        // column, v7 media table) are presence-probed by the migration, so
        // they re-land as no-ops.
        // created_at/updated_at must match mkLegacy: the sessions upsert
        // keeps created_at from the existing row (first-seen semantics), so
        // a mismatch would show up as sampled-verify drift, not a migration
        // problem.
        const { STORAGE_DDL_V1_FOR_TESTS } = await import("../src/storage/schema.ts");
        const v4Ddl = STORAGE_DDL_V1_FOR_TESTS.replace(/^\s*last_ingested_seq   INTEGER,$/m, "");
        const { DatabaseSync } = await import("node:sqlite");
        const raw = new DatabaseSync(dbPath);
        raw.exec(v4Ddl);
        raw.prepare(
            "INSERT INTO sessions (session_id, created_at, updated_at, next_block_id, next_run_id, meta_json, stats_json, metadata_json, kstate_json, tail_json, ccr_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ).run("v4-sess", 1000, 2000, 1, 1, "{}", "{}", "{}", "{}", '{"pluginSnapshotSeqs":null,"tailSeqs":[0],"tailFolded":false}', null);
        raw.prepare("INSERT INTO content (hash, byte_len, refcount, bytes) VALUES (?, ?, 1, ?)").run("c0", Buffer.byteLength(JSON.stringify(msg(0))), Buffer.from(JSON.stringify(msg(0)), "utf8"));
        raw.prepare("INSERT INTO messages (session_id, seq, msg_id, ref, ref_num, role, kind, content_hash) VALUES (?, 0, ?, NULL, NULL, 'user', 'message', ?)").run("v4-sess", msg(0).id, "c0");
        raw.exec("PRAGMA user_version = 4");
        raw.close();

        const store = UnifiedStore.open(dbPath);
        try {
            const ver = (store as unknown as { db: { prepare(q: string): { get(): unknown } } }).db.prepare("PRAGMA user_version").get() as { user_version: number };
            assert.equal(ver.user_version, 12, "open() migrates v4 → v12");
            const row = store.getSessionRow("v4-sess");
            assert.ok(row, "legacy session row survives the migration");
            assert.equal(row!.lastIngestedSeq, null, "legacy rows read back with a NULL watermark (≡ full ingest)");
            const messages = store.getMessages("v4-sess");
            assert.equal(messages.length, 1, "legacy message rows readable");
            assert.equal(messages[0].msgId, msg(0).id);

            // First save on a migrated DB: NULL watermark → full ingest →
            // watermark established; the second save deltas.
            const shadow = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, (r) => r as LegacySessionLike);
            assert.ok(shadow);
            try {
                const legacy = mkLegacy("v4-sess", [msg(0), msg(1)]);
                shadow.record(legacy);
                assert.equal(stats(shadow).deltaIngests, 0);
                const tail2 = [msg(0), msg(1), msg(2)];
                shadow.record(mkLegacy("v4-sess", tail2));
                assert.equal(stats(shadow).deltaIngests, 1, "migrated DB deltas from the first re-ingest on");
                assertDeltaMatchesFull(store, mkLegacy("v4-sess", tail2));
            } finally {
                shadow.close();
            }
        } finally {
            store.close();
        }

        // Version-gate no-op: a DB stamped at the current schema head
        // (an older build opening a newer file) must open without error and
        // leave user_version alone — the additive-column contract. The
        // probe version sits AT the current schema head so the no-op branch
        // still exercises after future bumps.
        const fwd = new DatabaseSync(dbPath);
        fwd.exec("PRAGMA user_version = 11");
        fwd.close();
        const older = UnifiedStore.open(dbPath);
        try {
            const ver2 = (older as unknown as { db: { prepare(q: string): { get(): unknown } } }).db.prepare("PRAGMA user_version").get() as { user_version: number };
            assert.equal(ver2.user_version, 12, "newer user_version is left untouched");
            assert.ok(older.getSessionRow("v4-sess"), "rows stay readable");
        } finally {
            older.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: restart handoff — a foreign writer's trust is never inherited (#3285, per-session prime)", () => {
    const dir = tmpdir("bili-wm-handoff-");
    try {
        // Live saves carry the raw tail (persist.ts guardedBuild) — that is
        // what makes a plan the complete log. Both processes save through
        // that production shape.
        const withRaw = (id: string, lastMessages: CoreMessage[]): [LegacySessionLike, { lastMessages: CoreMessage[] }] =>
            [mkLegacy(id, lastMessages), { lastMessages }];
        // Process A: ingests 10 messages, then exits mid-drain (a host
        // restart while the old instance still flushes — the #3285 shape).
        const a = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, (r) => r as LegacySessionLike);
        assert.ok(a, "first process must open");
        try {
            const msgsA = Array.from({ length: 10 }, (_, i) => msg(i));
            a.record(...withRaw("handoff", msgsA));
        } finally {
            a.close();
        }
        // Process B boots on the same directory. Its view is what the host
        // resent AFTER the restart: two tail turns are gone (the desktop
        // rebuilt history from its own store), so B's truth is shorter than
        // what A ingested.
        const b = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, (r) => r as LegacySessionLike);
        assert.ok(b, "second process must open");
        const storeB = UnifiedStore.open(path.join(dir, "index.db"));
        try {
            // 2026-11-21 per-session rework: open no longer clears watermarks
            // (the global reset made OPEN a write — the #7156 first-write
            // hazard). The foreign watermark is still on disk right after
            // open; the new process's FIRST touch of the session primes its
            // trust (drops exactly that session's foreign watermark) before
            // reading it — so the first save still takes the full path, which
            // the assertions below pin (deltaIngests stays 0, A's rows are
            // replaced, no drift).
            assert.ok(storeB.ingestWatermark("handoff"), "the foreign watermark is still on disk after open (open performs no write)");
            const view = [msg(0), msg(1), msg(2)];
            b.record(...withRaw("handoff", view));
            const s = stats(b);
            assert.equal(s.deltaIngests, 0, "first save after the handoff takes the full path");
            assert.equal(s.verifyDrift, 0, "no DIGEST DRIFT: the full path replaced A's rows");
            assert.equal(s.verifyOk, 1, "sampled verification passes on the first ingest");
            const rows = storeB.getMessages("handoff");
            assert.equal(rows.length, 3, "A's orphaned rows are gone");
            assert.deepEqual(rows.map((r) => r.msgId), ["h_msg0", "h_msg1", "h_msg2"]);
            // Steady state resumes against B's own watermark.
            b.record(...withRaw("handoff", [...view, msg(10)]));
            assert.equal(stats(b).deltaIngests, 1, "post-handoff saves delta again");
            assert.equal(stats(b).verifyDrift, 0);
        } finally {
            storeB.close();
            b.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

test("watermark: completeLog sweep skips lane-owned sessions (#3248/#3285)", () => {
    const dir = tmpdir("bili-wm-lane-skip-");
    try {
        const store = UnifiedStore.open(path.join(dir, "index.db"));
        try {
            // Lane-owned: any "inc:" ext key marks positional consumption of the
            // messages table — its rows must survive even when absent from the plan.
            store.ingestSession(planIngest(mkLegacy("lane-a", [msg(0), msg(1)])));
            store.setSessionExt("lane-a", "inc:base-seq", 2);
            store.ingestSession(planIngest(mkLegacy("lane-a", [msg(2)])), undefined, { completeLog: true });
            assert.deepEqual(store.getMessages("lane-a").map((r) => r.msgId), ["h_msg0", "h_msg1", "h_msg2"], "lane rows survive the sweep");
            // Control: without a lane marker the #3285 sweep still applies.
            store.ingestSession(planIngest(mkLegacy("plain-b", [msg(0), msg(1)])));
            store.ingestSession(planIngest(mkLegacy("plain-b", [msg(2)])), undefined, { completeLog: true });
            assert.deepEqual(store.getMessages("plain-b").map((r) => r.msgId), ["h_msg2"], "non-lane foreign rows swept");
        } finally {
            store.close();
        }
    } finally {
        rmBestEffort(dir);
    }
});

for (const size of [2, 4096]) test(`full replay skips refcount writes only for already stored messages (${size} rows)`, (t) => {
    const dir = tmpdir("bili-wm-replay-noop-");
    const store = UnifiedStore.open(path.join(dir, "index.db"));
    try {
        const db = (store as unknown as { db: ReturnType<typeof openSqliteDatabase>["db"] }).db;
        const id = "replay-noop";
        const history = Array.from({ length: size }, (_, i) => msg(i));
        const save = (tail: CoreMessage[]) => store.ingestSession(planIngest(mkLegacy(id, tail)), undefined, { completeLog: true });
        const cas = () => db.prepare("SELECT hash, refcount FROM content ORDER BY hash").all();
        assert.equal(save(history), true);
        const rows = store.getMessages(id);
        const before = cas();
        const prepare = db.prepare.bind(db);
        const writes: string[] = [];
        let batchQueries = 0;
        t.mock.method(db, "prepare", (sql: string) => {
            const statement = prepare(sql);
            if (sql.includes("FROM json_each(?) AS j")) {
                const all = statement.all.bind(statement);
                t.mock.method(statement, "all", (...args: unknown[]) => {
                    batchQueries++;
                    return all(...args);
                });
            }
            if (/^(?:INSERT INTO content|UPDATE content SET refcount|INSERT OR IGNORE INTO messages)/.test(sql)) {
                const run = statement.run.bind(statement);
                t.mock.method(statement, "run", (...args: unknown[]) => {
                    writes.push(sql);
                    return run(...args);
                });
            }
            return statement;
        });
        assert.equal(save(history), true);
        assert.equal(writes.length, 0);
        assert.equal(batchQueries, size === 4096 ? 8 : 0);
        assert.deepEqual(store.getMessages(id), rows);
        assert.deepEqual(cas(), before);

        assert.equal(save([...history, msg(size)]), true);
        assert.equal(writes.filter(sql => sql.startsWith("INSERT OR IGNORE INTO messages")).length, 1);
        assert.equal(store.getMessages(id).length, size + 1);
        assert.equal(cas().length, before.length + 1);
    } finally {
        store.close();
        rmBestEffort(dir);
    }
});

test("bounded remaps restore retired media at its old seq and append past omitted history", (t) => {
    const dir = tmpdir("bili-wm-bounded-remap-");
    const store = UnifiedStore.open(path.join(dir, "index.db"));
    try {
        const id = "wm-bounded-remap";
        const photo = { ...msg(1), imageBase64: "QUJD".repeat(700) };
        const initial = Array.from({ length: 180 }, (_, i) => i === 1 ? photo : msg(i));
        const save = (messages: CoreMessage[], completeLog: boolean) => {
            assert.equal(store.ingestSession(planIngest(mkLegacy(id, messages)), undefined, { completeLog }), true);
        };
        save(initial, true);
        const before = store.getMessages(id);
        assert.match(before[1].content, /QUJD/);

        const db = (store as unknown as { db: ReturnType<typeof openSqliteDatabase>["db"] }).db;
        const prepare = db.prepare.bind(db);
        const queries: string[] = [];
        t.mock.method(db, "prepare", (sql: string) => {
            queries.push(sql);
            return prepare(sql);
        });
        const latest = initial.at(-1)!;
        save([latest], true);
        assert.equal(queries.some((sql) => sql.includes("INDEXED BY idx_messages_msg_id")), false);
        assert.ok(queries.some((sql) => sql.includes("FROM messages WHERE session_id = ? ORDER BY seq")));
        assert.deepEqual(store.getMessages(id).map(({ seq }) => seq), [179]);

        queries.length = 0;
        const appended = msg(180);
        save([photo, appended], false);
        assert.ok(queries.some((sql) => sql.includes("INDEXED BY idx_messages_msg_id")));
        assert.equal(queries.some((sql) => sql.includes("FROM messages WHERE session_id = ? ORDER BY seq")), false);
        const after = store.getMessages(id);
        assert.deepEqual(after.map(({ seq }) => seq), [1, 179, 180]);
        assert.equal(after[0].content, before[1].content);
        assert.equal(after[0].contentHash, before[1].contentHash);
        assert.deepEqual(store.ingestWatermark(id), { seq: 180, tailSeqs: [1, 180] });
        assert.equal(store.getMessages(id, { includeRetired: true }).length, 181);
    } finally {
        store.close();
        rmBestEffort(dir);
    }
});

test("large ref ledgers compare the whole stored prefix and fall back for edits and pruning", (t) => {
    const dir = tmpdir("bili-wm-ref-prefix-");
    const store = UnifiedStore.open(path.join(dir, "index.db"));
    try {
        const db = (store as unknown as { db: ReturnType<typeof openSqliteDatabase>["db"] }).db;
        const prepare = db.prepare.bind(db);
        const queries: string[] = [];
        t.mock.method(db, "prepare", (sql: string) => {
            queries.push(sql);
            return prepare(sql);
        });
        const legacy = mkLegacy("large-ref-ledger", [msg(0)]);
        const byRaw = legacy.state.messageRefs.byRaw;
        for (let i = 1; i <= 8200; i++) byRaw[`ref-${i}`] = `m${String(i).padStart(5, "0")}`;
        byRaw['special "\\😀'] = "BLOCKED";
        const save = (expectFallback: boolean) => {
            const plan = planIngest(legacy);
            queries.length = 0;
            assert.equal(store.ingestSession(plan), true);
            assert.ok(queries.some((sql) => sql.includes("json_group_array(json_array(ord")));
            assert.equal(queries.some((sql) => sql.includes("SELECT raw_id, ord, ref, ref_num FROM refs")), expectFallback);
            assert.deepEqual(store.getRefs(legacy.id), plan.refs);
        };

        save(true);
        save(false);
        byRaw.appended = "m08201";
        save(false);
        prepare("UPDATE refs SET ref = ?, ref_num = NULL WHERE session_id = ? AND raw_id = ?").run("BLOCKED", legacy.id, "ref-4100");
        save(true);
        delete byRaw["ref-3000"];
        save(true);
        save(false);
    } finally {
        store.close();
        rmBestEffort(dir);
    }
});

test("delta saves reuse unchanged block coverage bounds and committed ref prefixes, falling back on edits or newly covered messages", (t) => {
    const dir = tmpdir("bili-wm-delta-reuse-");
    const store = UnifiedStore.open(path.join(dir, "index.db"));
    try {
        const db = (store as unknown as { db: ReturnType<typeof openSqliteDatabase>["db"] }).db;
        const prepare = db.prepare.bind(db);
        const queries: string[] = [];
        t.mock.method(db, "prepare", (sql: string) => {
            queries.push(sql);
            return prepare(sql);
        });
        const msgs = [msg(0), msg(1), msg(2)];
        const legacy = mkLegacy("delta-reuse-session", msgs);
        const byRaw = legacy.state.messageRefs.byRaw;
        for (let i = 1; i <= 8200; i++) byRaw[`ref-${i}`] = `m${String(i).padStart(5, "0")}`;
        legacy.state.blocks = [{
            blockId: "b1",
            summary: "summary 1",
            effectiveMessageIds: [msgs[0]!.id, msgs[1]!.id],
            directMessageIds: [msgs[0]!.id, msgs[1]!.id],
            startRef: "m00001",
            endRef: "m00002",
            tier: 1,
            active: true,
            createdAt: 1000,
        } as unknown as (typeof legacy.state.blocks)[number]];
        legacy.blockContents.set("b1", { one: { text: "one", count: 2 }, full: { text: "full", count: 2 } });

        // Initial full ingest establishes bounds [0, 1] and committed ref prefix.
        assert.equal(store.ingestSession(planIngest(legacy), undefined, { completeLog: true }), true);
        const b1Initial = store.getBlocks(legacy.id);
        assert.equal(b1Initial[0]!.startSeq, 0);
        assert.equal(b1Initial[0]!.endSeq, 1);

        // Delta save appending 1 uncovered message + 1 ref skips block span scan AND ref json_group_array.
        const m3 = msg(3);
        legacy.lastMessages = [...msgs, m3];
        byRaw[m3.id] = "m08201";
        const mark1 = store.ingestWatermark(legacy.id)!;
        const boundary1 = store.probeIngestBoundary(legacy.id, mark1.seq)!;
        const deltaPlan1 = planIngestDelta(legacy, mark1.seq, mark1.tailSeqs, { msgId: boundary1.msgId, contentHash: boundary1.contentHash }, true)!;
        queries.length = 0;
        assert.equal(store.ingestSession(deltaPlan1, {
            trustedSeq: mark1.seq,
            boundaryMsgId: boundary1.msgId,
            boundaryHash: boundary1.contentHash,
            trustedTailSeqs: mark1.tailSeqs,
        }), true);
        assert.equal(queries.some((sql) => sql.includes("json_group_array(json_array(ord")), false, "delta append skips full ref prefix JSON aggregation");
        assert.equal(queries.some((sql) => sql.includes("FROM messages WHERE session_id = ? AND msg_id IN")), false, "unchanged block skips covered-seq query");
        assert.equal(queries.some((sql) => sql.includes("SELECT msg_id, seq, retired_at FROM messages")), false, "unchanged block skips full messages scan");
        assert.deepEqual(store.getRefs(legacy.id), deltaPlan1.refs);

        // Counterfactual 1: adding a new block on a delta save resolves its seq bounds accurately.
        const m4 = msg(4);
        legacy.lastMessages = [...legacy.lastMessages, m4];
        byRaw[m4.id] = "m08202";
        legacy.state.blocks.push({
            blockId: "b2",
            summary: "summary 2",
            effectiveMessageIds: [msgs[2]!.id, m3.id],
            directMessageIds: [msgs[2]!.id, m3.id],
            startRef: "m00003",
            endRef: "m08201",
            tier: 1,
            active: true,
            createdAt: 2000,
        } as unknown as (typeof legacy.state.blocks)[number]);
        legacy.blockContents.set("b2", { one: { text: "one2", count: 2 }, full: { text: "full2", count: 2 } });
        const mark2 = store.ingestWatermark(legacy.id)!;
        const boundary2 = store.probeIngestBoundary(legacy.id, mark2.seq)!;
        const deltaPlan2 = planIngestDelta(legacy, mark2.seq, mark2.tailSeqs, { msgId: boundary2.msgId, contentHash: boundary2.contentHash }, true)!;
        assert.equal(store.ingestSession(deltaPlan2, {
            trustedSeq: mark2.seq,
            boundaryMsgId: boundary2.msgId,
            boundaryHash: boundary2.contentHash,
            trustedTailSeqs: mark2.tailSeqs,
        }), true);
        const blocksAfter2 = store.getBlocks(legacy.id);
        assert.equal(blocksAfter2[1]!.blockId, "b2");
        assert.equal(blocksAfter2[1]!.startSeq, 2);
        assert.equal(blocksAfter2[1]!.endSeq, 3);

        // Counterfactual 2: pruning a ref on a delta save falls back and removes the deleted ref.
        delete byRaw["ref-100"];
        const m5 = msg(5);
        legacy.lastMessages = [...legacy.lastMessages, m5];
        byRaw[m5.id] = "m08203";
        const mark3 = store.ingestWatermark(legacy.id)!;
        const boundary3 = store.probeIngestBoundary(legacy.id, mark3.seq)!;
        const deltaPlan3 = planIngestDelta(legacy, mark3.seq, mark3.tailSeqs, { msgId: boundary3.msgId, contentHash: boundary3.contentHash }, true)!;
        assert.equal(store.ingestSession(deltaPlan3, {
            trustedSeq: mark3.seq,
            boundaryMsgId: boundary3.msgId,
            boundaryHash: boundary3.contentHash,
            trustedTailSeqs: mark3.tailSeqs,
        }), true);
        assert.deepEqual(store.getRefs(legacy.id), deltaPlan3.refs);

        // Full completeLog ingest reuses remapPlanToAbsolute's full-table rows
        // for both the stale-row sweep and covered-seq span resolution, avoiding
        // two redundant full scans of messages while retiring/reactivating accurately.
        const m6 = msg(6);
        // Drop m5 (should retire) and append m6 (should insert live + cover in b3).
        legacy.lastMessages = [...legacy.lastMessages.slice(0, -1), m6];
        byRaw[m6.id] = "m08204";
        legacy.state.blocks.push({
            blockId: "b3",
            summary: "summary 3",
            effectiveMessageIds: [m4.id, m6.id],
            directMessageIds: [m4.id, m6.id],
            startRef: "m08202",
            endRef: "m08204",
            tier: 1,
            active: true,
            createdAt: 3000,
        } as unknown as (typeof legacy.state.blocks)[number]);
        legacy.blockContents.set("b3", { one: { text: "one3", count: 2 }, full: { text: "full3", count: 2 } });
        queries.length = 0;
        store.ingestSession(planIngest(legacy), undefined, { completeLog: true });
        assert.equal(queries.some((sql) => sql.includes("SELECT seq, msg_id, identity, retired_at FROM messages WHERE session_id = ?")), false, "completeLog sweep reuses remap rows");
        assert.equal(queries.some((sql) => sql.includes("SELECT msg_id, seq, retired_at FROM messages WHERE session_id = ?")), false, "full ingest covered-seq bounds reuse remap + inserted rows");
        assert.equal(queries.some((sql) => sql.includes("SELECT msg_id, seq FROM messages WHERE session_id = ? AND msg_id IN")), false, "full ingest covered-seq bounds skip batched IN queries");
        const liveAfterFull = store.getMessages(legacy.id).map((row) => row.msgId);
        assert.equal(liveAfterFull.includes(m5.id), false, "dropped m5 retired by reused sweep");
        assert.equal(liveAfterFull.includes(m6.id), true, "new m6 inserted live");
        const b3 = store.getBlocks(legacy.id).find((b) => b.blockId === "b3");
        assert.ok(b3);
        const m4Seq = store.getMessages(legacy.id).find((row) => row.msgId === m4.id)!.seq;
        const m6Seq = store.getMessages(legacy.id).find((row) => row.msgId === m6.id)!.seq;
        assert.equal(b3.startSeq, m4Seq);
        assert.equal(b3.endSeq, m6Seq);

        // Reactivate m5 via a second full completeLog ingest and verify bounds include it again.
        legacy.lastMessages = [...legacy.lastMessages, m5];
        queries.length = 0;
        store.ingestSession(planIngest(legacy), undefined, { completeLog: true });
        assert.equal(queries.some((sql) => sql.includes("SELECT seq, msg_id, identity, retired_at FROM messages WHERE session_id = ?")), false);
        assert.equal(store.getMessages(legacy.id).some((row) => row.msgId === m5.id), true, "m5 reactivated cleanly");

        // Steady-state delta after a full ingest uses the committed block
        // snapshot and skips reading payload_json from blocks altogether.
        const m7 = msg(7);
        legacy.lastMessages = [...legacy.lastMessages, m7];
        byRaw[m7.id] = "m08205";
        const mark4 = store.ingestWatermark(legacy.id)!;
        const boundary4 = store.probeIngestBoundary(legacy.id, mark4.seq)!;
        const deltaPlan4 = planIngestDelta(legacy, mark4.seq, mark4.tailSeqs, { msgId: boundary4.msgId, contentHash: boundary4.contentHash }, true)!;
        queries.length = 0;
        assert.equal(store.ingestSession(deltaPlan4, {
            trustedSeq: mark4.seq,
            boundaryMsgId: boundary4.msgId,
            boundaryHash: boundary4.contentHash,
            trustedTailSeqs: mark4.tailSeqs,
        }), true);
        assert.equal(
            queries.some((sql) => sql.includes("payload_json FROM blocks WHERE session_id = ?")),
            false,
            "steady-state delta save must skip full blocks payload_json read via committed block snapshot",
        );
    } finally {
        store.close();
        rmBestEffort(dir);
    }
});
