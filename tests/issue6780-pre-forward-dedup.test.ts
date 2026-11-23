import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { SessionStore } from "../src/persist.ts";
import { getCacheLedger } from "../src/cache-ledger.ts";
import { buildRefRows, serializeSessionMetadata } from "../src/storage/ingest.ts";
import { snapshotMessages, type Session } from "../src/session.ts";
import type { CoreMessage } from "acp-kernel";

/**
 * #6780 P0: with the pure-SQLite default (#6480, JSON writer off) the prepare
 * phase runs TWO full unified-index ingests per turn BEFORE the model call —
 * the synchronous durability ingest (#6326, ingestRowsNow) and the coalesced
 * writer-off save armed by prepare's own markDirty (5a80e2b8 removed the
 * debounce on this lane). The second one is a no-op delta (the watermark was
 * already advanced) that still pays the three-table full reads, the
 * whole-record re-serialization and a second commit round-trip — ~1.3s of
 * #6780's pre-forward cost on the reporter's 23k-message session.
 *
 * Pinned here (writer-off mode, exactly what the prepare phase does):
 *   1. When nothing changed between ingestRowsNow and the coalesced build,
 *      the build SKIPS the heavy legs — the sidecar record() is not called a
 *      second time — but the settle protocol still lands (lastSaveLanded).
 *   2. When the state DID change after the arm (re-snapshot + markDirty),
 *      the full build still runs — the arm is a hint, never a commitment.
 *   3. (review F6) A lastMessages re-snapshot ALONE (no kernel mutation, one
 *      markDirty) defeats the skip — isolating the ref-swap gate condition.
 *   4. (review F1) A fold-turn ledger align consumed during scheduleSave
 *      restamps refs AFTER the arm; the coalesced build must take the full
 *      path (the #4733 same-record invariant), i.e. the align path consumes
 *      the arm.
 *   5. (review F2) The arm is single-shot: once the coalesced build has made
 *      its decision, a LATER standalone markDirty must not be able to skip on
 *      the stale arm.
 */

process.env.BILI_PERSIST_ZSTD = "0";
// Same hermeticity pin as the other storage suites: pin BOTH tiers so every
// leg controls its own mode explicitly (absent now defaults to full, #3818).
const hermetic = fs.mkdtempSync(path.join(os.tmpdir(), "bili-6780-hermetic-"));
process.env.BILI_CONFIG_FILE = path.join(hermetic, "no-config.json");
process.env.BILI_STORAGE_UNIFIED = "legacy";

const CORPUS_ROOT = path.resolve(import.meta.dirname, "golden/storage-corpus");

function loadCorpusCase(dir: string, name: string, id: string, protocol: string) {
    fs.cpSync(path.join(CORPUS_ROOT, name), path.join(dir, name), { recursive: true });
    const loader = new SessionStore({ dir: path.join(dir, name), debounceMs: 1, enabled: true });
    const loaded = loader.loadSync(id, { protocol, upstreamOrigin: "http://127.0.0.1:8199" });
    loader.cancelAll();
    loader.closeQuietly();
    assert.ok(loaded, `${name}: corpus loadSync returned null`);
    return loaded as Session;
}

/** Windows: an ephemeral stray handle must not fail CI — best-effort delete
 *  with bounded retries, swallowed on persistent EBUSY/EPERM. */
function rmBestEffort(p: string): void {
    try {
        fs.rmSync(p, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch {
        /* leaked temp dir on a Windows runner is harmless */
    }
}

type Count = () => number;

interface Harness {
    session: Session;
    /** The synchronous pre-forward durability ingest (prepare phase step). */
    ingestRowsNow: () => void;
    /** Arm the sidecar record() spy (call right after the first
     *  ingestRowsNow). Returns the ingest counter. */
    spy: () => Count;
    shadowStats: () => { ingests: number; verifySamples: number; verifyOk: number; verifyDrift: number };
    /** Bump the epoch exactly like session.ts markDirty does and schedule the
     *  save (calling session.markDirty would hit the module-singleton store,
     *  not the harness store). */
    markDirty: () => void;
    /** The writer-off lane's scheduleSave without an epoch bump (e.g. the
     *  incremental-lane close-out shape). */
    scheduleSave: () => void;
    lastSaveLanded: Map<string, boolean>;
    /** Let the coalesced microtask run. */
    settle: () => Promise<void>;
}

async function withHarness(run: (h: Harness) => Promise<void>): Promise<void> {
    const prevUnified = process.env.BILI_STORAGE_UNIFIED;
    const prevWriter = process.env.BILI_STORAGE_JSON_WRITER;
    process.env.BILI_STORAGE_UNIFIED = "full";
    process.env.BILI_STORAGE_JSON_WRITER = "off"; // no envelope — the writer-off coalesced lane
    const seed = fs.mkdtempSync(path.join(os.tmpdir(), "bili-6780-seed-"));
    const session = loadCorpusCase(seed, "folded", "corpus-folded", "anthropic");
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bili-6780-dedup-"));
    try {
        const store = new SessionStore({ dir, debounceMs: 60_000, enabled: true });
        const anyStore = store as unknown as {
            shadow: {
                record: (...args: unknown[]) => unknown;
                stats: { ingests: number; verifySamples: number; verifyOk: number; verifyDrift: number };
            };
            scheduleSave: (s: Session) => void;
            lastSaveLanded: Map<string, boolean>;
            ingestRowsNow: (s: Session) => void;
        };
        let ingests = 0;
        const harness: Harness = {
            session,
            ingestRowsNow: () => anyStore.ingestRowsNow(session),
            spy: () => {
                const real = anyStore.shadow.record.bind(anyStore.shadow);
                anyStore.shadow.record = (...args: unknown[]) => { ingests++; return real(...args); };
                return () => ingests;
            },
            shadowStats: () => ({ ...anyStore.shadow.stats }),
            markDirty: () => {
                session.revisionEpoch = (session.revisionEpoch ?? 0) + 1;
                anyStore.scheduleSave(session);
            },
            scheduleSave: () => anyStore.scheduleSave(session),
            lastSaveLanded: anyStore.lastSaveLanded,
            settle: () => new Promise((r) => setTimeout(r, 50)),
        };
        await run(harness);
        store.cancelAll();
        store.closeQuietly();
    } finally {
        rmBestEffort(dir);
        rmBestEffort(seed);
        process.env.BILI_STORAGE_UNIFIED = prevUnified;
        process.env.BILI_STORAGE_JSON_WRITER = prevWriter;
    }
}

test("#6780 P0: the writer-off coalesced save skips the duplicate ingest when ingestRowsNow already covered this tick's state", async () => {
    await withHarness(async ({ session, ingestRowsNow, spy, markDirty, lastSaveLanded, settle }) => {
        // Exactly what the prepare phase does before the model call: the
        // synchronous durability ingest, then the plain markDirty.
        ingestRowsNow();
        const ingests = spy();

        markDirty();
        await settle();
        assert.equal(ingests(), 0, "state unchanged since ingestRowsNow — the coalesced save must NOT re-run the full rebuild + full delta-ingest");
        assert.equal(lastSaveLanded.get(session.id), true, "the settle protocol must still land on the skip path");

        // Counterfactual: the state changes AFTER the arm (a re-snapshot
        // swaps lastMessages without bumping the epoch; the follow-up
        // markDirty bumps it) → the full build must still run.
        snapshotMessages(session, [...(session.lastMessages ?? []), { id: "m-dedup-probe", role: "user", contentType: "text", text: "probe" } as unknown as CoreMessage]);
        markDirty();
        await settle();
        assert.equal(ingests(), 1, "a changed state must still take the full build (the arm never commits)");
    });
});

test("#6780 P0 (F6): a lastMessages re-snapshot alone (single markDirty) defeats the skip", async () => {
    await withHarness(async ({ session, ingestRowsNow, spy, markDirty, settle }) => {
        ingestRowsNow();
        const ingests = spy();

        // A fresh client request re-snapshots the raw messages: a NEW array
        // reference, no kernel mutation, no epoch bump — then only
        // prepare's own markDirty. The ref-swap condition must alone force
        // the full build (epoch is exactly armed.epoch + 1, so every other
        // gate condition passes).
        snapshotMessages(session, [...(session.lastMessages ?? []), { id: "m-ref-swap", role: "user", contentType: "text", text: "ref swap" } as unknown as CoreMessage]);
        markDirty();
        await settle();
        assert.equal(ingests(), 1, "a re-snapshotted lastMessages (same epoch delta) must take the full build");
    });
});

test("#6780 P0 (F1): a fold-turn ledger align consumed during scheduleSave forces the full build", async () => {
    await withHarness(async ({ session, ingestRowsNow, spy, markDirty, lastSaveLanded, settle }) => {
        ingestRowsNow();
        const ingests = spy();

        // A fold commit between the previous save and this prepare set the
        // align flag. ingestRowsNow does NOT consume it — scheduleSave's
        // entry does, and the restamp mutates state (ref renumbering + CCR
        // re-key) AFTER the arm was set.
        session.pendingLedgerAlign = true;
        markDirty();
        await settle();
        assert.equal(ingests(), 1, "the restamped state must take the full build — the align path consumes the arm (the #4733 same-record invariant)");
        assert.equal(lastSaveLanded.get(session.id), true, "the settle protocol must land on the forced full build");
        assert.equal(session.pendingLedgerAlign, undefined, "the align flag is consumed exactly once");
    });
});

test("#6780 P0 (F2): the arm is single-shot — a later standalone markDirty cannot skip on the consumed arm", async () => {
    await withHarness(async ({ session, ingestRowsNow, spy, markDirty, scheduleSave, settle }) => {
        ingestRowsNow(); // arms at epoch E
        const ingests = spy();

        // A scheduleSave WITHOUT the epoch bump (the incremental-lane
        // close-out shape): the gate fails (E !== E + 1) → full build, and
        // the arm is consumed by this run's decision.
        scheduleSave();
        await settle();
        assert.equal(ingests(), 1, "epoch unchanged since the arm → full build");

        // Now a single non-metadata mutation + markDirty reaches E+1 — the
        // OLD (never-consumed) arm would match the whole gate and wrongly
        // skip this one. The single-shot arm must not allow that.
        session.stats.outputTokens += 1;
        markDirty();
        await settle();
        assert.equal(ingests(), 2, "a stale arm must not justify skipping a save it never covered");
    });
});

test("ingestRowsNow defers sampled read-back verification to the next non-deferred save", async () => {
    await withHarness(async ({ session, ingestRowsNow, shadowStats, markDirty, settle }) => {
        ingestRowsNow();
        assert.equal(shadowStats().ingests, 1, "durability ingest committed synchronously");
        assert.equal(shadowStats().verifySamples, 0, "pre-send ingestRowsNow must defer sampled read-back verify off TTFT");

        // Same-tick prepare markDirty skips via durability arm (still deferred).
        markDirty();
        await settle();
        assert.equal(shadowStats().verifySamples, 0, "skipped coalesced build keeps verify deferred until a real post-turn save");

        // Post-turn save runs the deferred sampled verify.
        session.stats.outputTokens += 1;
        markDirty();
        await settle();
        assert.equal(shadowStats().verifySamples, 1, "next non-deferred save runs the deferred verify sample");
        assert.equal(shadowStats().verifyOk, 1, "deferred verify sample must pass");
        assert.equal(shadowStats().verifyDrift, 0, "no drift");
    });
});

test("#6780 P0: durability skip detects both regular metadata mutations and replaced reconcile snapshot keys", async () => {
    await withHarness(async ({ session, ingestRowsNow, spy, markDirty, settle }) => {
        session.metadata.foldAnchors = { a: { n: "h1" } };
        ingestRowsNow();
        const ingests = spy();

        // Replacing a pinned reconcile key defeats the skip.
        session.metadata.foldAnchors = { a: { n: "h2" } };
        markDirty();
        await settle();
        assert.equal(ingests(), 1, "replacing foldAnchors must defeat the durability skip");

        // Re-arm and mutate a non-pinned metadata key (e.g. probeFlag).
        ingestRowsNow();
        const before = ingests();
        session.metadata.probeFlag = 1;
        markDirty();
        await settle();
        assert.equal(ingests(), before + 1, "mutating a regular metadata key must defeat the durability skip");

        // Re-arm with a live cacheLedger, verify unchanged skips, and mutating via getCacheLedger defeats the skip.
        const led = getCacheLedger(session);
        ingestRowsNow();
        const beforeUnchanged = ingests();
        markDirty();
        await settle();
        assert.equal(ingests(), beforeUnchanged, "unchanged cacheLedger keeps the durability skip active");

        ingestRowsNow();
        const beforeMutated = ingests();
        getCacheLedger(session).sampleSeq += 1;
        markDirty();
        await settle();
        assert.equal(ingests(), beforeMutated + 1, "mutating cacheLedger via getCacheLedger must defeat the durability skip");
        assert.equal(led.sampleSeq, 1);
    });
});

test("serializeSessionMetadata and buildRefRows match uncached serialization across mutations", () => {
    const fakeSession = { id: "s1", metadata: {}, state: { blocks: [] } } as unknown as Parameters<typeof getCacheLedger>[0];
    const led = getCacheLedger(fakeSession);
    const anchors = { a: { n: "h1", c: "text" } };
    const order = ["a", "b"];
    const positions = { ids: ["a", "b"], canon: ["c1", "c2"] };
    const meta: Record<string, unknown> = {
        cacheLedger: led,
        foldAnchors: anchors,
        foldAnchorOrder: order,
        foldPositions: positions,
        small: "v1",
        omitted: undefined,
    };
    assert.equal(serializeSessionMetadata(meta), JSON.stringify(meta), "cold serialization matches JSON.stringify byte-for-byte");
    assert.equal(serializeSessionMetadata(meta), JSON.stringify(meta), "warm cache hit matches JSON.stringify byte-for-byte");

    // Mutate cacheLedger via getCacheLedger and directly; both must invalidate.
    getCacheLedger(fakeSession).sampleSeq += 1;
    assert.equal(serializeSessionMetadata(meta), JSON.stringify(meta), "getCacheLedger mutation invalidates cached JSON");
    led.lines.push({ seq: 1, at: 100, input: 10, cached: 5, output: 2, hitPct: 50, tr: 0, cr: 0, nc: 5 } as never);
    assert.equal(serializeSessionMetadata(meta), JSON.stringify(meta), "direct structural cacheLedger mutation invalidates cached JSON");

    // Replace foldAnchors / extend foldAnchorOrder / mutate small key.
    meta.foldAnchors = { a: { n: "h2", c: "text" } };
    order.push("c");
    meta.small = "v2";
    assert.equal(serializeSessionMetadata(meta), JSON.stringify(meta), "fold snapshot replacement and array append stay byte-identical");

    // buildRefRows: cold, warm identical, append, edit, delete.
    const byRaw: Record<string, string> = { r1: "m00001", r2: "m00002" };
    const expected = () => Object.entries(byRaw).map(([rawId, ref], ord) => ({ ord, rawId, ref, refNum: Number(ref.slice(1)) }));
    assert.deepEqual(buildRefRows(byRaw), expected());
    assert.deepEqual(buildRefRows(byRaw), expected());
    byRaw.r3 = "m00003";
    assert.deepEqual(buildRefRows(byRaw), expected());
    byRaw.r2 = "m00099";
    assert.deepEqual(buildRefRows(byRaw), expected());
    delete byRaw.r1;
    assert.deepEqual(buildRefRows(byRaw), expected());
});
