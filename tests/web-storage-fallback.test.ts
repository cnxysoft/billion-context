import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createInitialState } from "acp-kernel";
import type { CompressionBlock } from "acp-kernel";
import { LazyBlockContents, _resetSessionsForTest, getSession } from "../src/session.ts";
import { UnifiedStore } from "../src/storage/store.ts";
import { planIngest, toLegacyLike } from "../src/storage/ingest.ts";
import { _resetDiskCacheForTest, buildMessageContent, buildSessionDetail } from "../src/web/sessions-data.ts";

process.env.BILI_PERSIST_ZSTD = "0";
process.env.BILI_STORAGE_UNIFIED = "full";
process.env.BILI_SESSIONS_DIR = mkdtempSync(path.join(tmpdir(), "bili-web-fallback-sess-"));
process.env.BILI_CONFIG_FILE = path.join(mkdtempSync(path.join(tmpdir(), "bili-web-fallback-cfg-")), "no-config.json");

function mkBlock(blockId: string, direct: string[], effective: string[]): CompressionBlock {
    return {
        blockId,
        runId: "run-1",
        tier: 1 as CompressionBlock["tier"],
        summary: "s",
        directMessageIds: direct,
        effectiveMessageIds: effective,
        directBlockIds: [],
        compressedTokens: 1000,
        createdAt: 1_700_000_000_000,
        survivedCount: 0,
        generation: "young",
        active: true,
    };
}

test("#4738: messages pruned from the resident window hydrate previews + full text from storage", async () => {
    _resetSessionsForTest();
    const s = getSession("fallback-msgs", { protocol: "openai" });
    s.state = createInitialState();
    s.state.blocks = [mkBlock("b1", ["rm1", "rm2", "rm3"], ["rm1", "rm2", "rm3"])];
    s.state.messageRefs = {
        byRaw: { rm1: "m00001", rm2: "m00002", rm3: "m00099" },
        byRef: { m00001: "rm1", m00002: "rm2", m00099: "rm3" },
    };
    s.lastMessages = [
        { id: "rm1", role: "user", contentType: "text", text: "hello   there\nworld" },
        { id: "rm2", role: "assistant", contentType: "text", text: "stored answer" },
    ];
    s.blockContents = new LazyBlockContents("fallback-msgs", {}, () => null);

    // Seed the unified index exactly the way a save would have. Then the live
    // session forgets the bytes — the same observable state as host-side native
    // compaction pruning `lastMessages` client-side (rm3 was never stored).
    const dbPath = path.join(process.env.BILI_SESSIONS_DIR!, "index.db");
    const seed = UnifiedStore.open(dbPath);
    try {
        seed.ingestSession(planIngest(toLegacyLike(s)));
    } finally {
        seed.close();
    }
    s.lastMessages = [{ id: "rm1", role: "user", contentType: "text", text: "hello   there\nworld" }];
    const db = new DatabaseSync(dbPath);
    const row = db.prepare("SELECT content_hash, identity FROM messages WHERE session_id = ? AND msg_id = ?").get(s.id, "rm2");
    db.close();
    assert.ok(row && typeof row.content_hash === "string" && typeof row.identity === "string");
    const alias = { rawId: "rm2", contentHash: row.content_hash, identity: row.identity };
    s.metadata.recoveredMessageRefAliases = {
        m00001: alias,
        m00008: alias,
        m00009: { ...alias, rawId: "rm3" },
        m00010: { ...alias, rawId: "unregistered" },
        m00011: { ...alias, identity: "0".repeat(64) },
        m00012: { ...alias, contentHash: "0".repeat(64) },
    };
    _resetDiskCacheForTest();
    try {
        const d = await buildSessionDetail("fallback-msgs");
        assert.ok(d);
        assert.deepEqual(d.blockDetails[0].directMessages, [
            { ref: "m00001", role: "user", preview: "hello there world" },
            { ref: "m00002", role: "assistant", preview: "stored answer" }, // pruned from the window → hydrated, NOT missing
            { ref: "m00099", missing: true }, // stamped by the ledger but absent from storage too → honest miss
        ]);
        assert.equal(d.blockDetails[0].directMessageCount, 3);
        assert.deepEqual(d.blockDetails[0].directMessageRefs, ["m00001", "m00002", "m00099"]);

        const one = await buildMessageContent("fallback-msgs", "m00002");
        assert.ok(one.ok, "click-to-open serves a pruned message from storage");
        assert.equal(one.role, "assistant");
        assert.equal(one.text, "stored answer");
        assert.equal(one.totalBytes, Buffer.byteLength("stored answer"));

        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00008"), { ...one, ref: "m00008" });
        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00009"), { ok: false, reason: "unknown-message" });
        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00010"), { ok: false, reason: "unknown-message" });
        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00011"), { ok: false, reason: "unknown-message" });
        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00012"), { ...one, ref: "m00012" });
        const update = new DatabaseSync(dbPath);
        try {
            update.prepare("UPDATE messages SET retired_at = 1 WHERE session_id = ? AND msg_id = ?").run(s.id, "rm2");
        } finally {
            update.close();
        }
        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00008"), { ...one, ref: "m00008" });
        assert.equal(s.state.messageRefs.byRef.m00008, undefined);

        const resident = await buildMessageContent("fallback-msgs", "m00001");
        assert.ok(resident.ok);
        assert.equal(resident.text, "hello   there\nworld", "resident bytes stay verbatim — fallback never rewrites them");

        assert.deepEqual(await buildMessageContent("fallback-msgs", "m00099"), { ok: false, reason: "unknown-message" });
        assert.deepEqual(await buildMessageContent("no-such-session", "m00001"), { ok: false, reason: "unknown-session" });
    } finally {
        _resetDiskCacheForTest();
    }
});

test("#4738 fail-open: with no unified store the pruned leaf stays an honest miss", async () => {
    process.env.BILI_STORAGE_UNIFIED = "legacy";
    _resetSessionsForTest();
    const s = getSession("fallback-miss", { protocol: "openai" });
    s.state = createInitialState();
    s.state.blocks = [mkBlock("b1", ["rm1"], ["rm1"])];
    s.state.messageRefs = { byRaw: { rm1: "m00001" }, byRef: { m00001: "rm1" } };
    s.lastMessages = [];
    s.blockContents = new LazyBlockContents("fallback-miss", {}, () => null);
    _resetDiskCacheForTest();
    try {
        const d = await buildSessionDetail("fallback-miss");
        assert.ok(d);
        assert.deepEqual(d.blockDetails[0].directMessages, [{ ref: "m00001", missing: true }]);
        assert.deepEqual(await buildMessageContent("fallback-miss", "m00001"), { ok: false, reason: "unknown-message" });
    } finally {
        process.env.BILI_STORAGE_UNIFIED = "full";
        _resetDiskCacheForTest();
    }
});

test("#6029: leaves whose live ref was restamped onto a fresh number still hydrate from storage", async () => {
    _resetSessionsForTest();
    const s = getSession("restamp-msgs", { protocol: "openai" });
    s.state = createInitialState();
    const block = mkBlock("b1", ["rm1", "rm2", "rm3"], ["rm1", "rm2", "rm3"]);
    s.state.blocks = [block];
    // Seed-time state: refs are ALIGNED with position (ref_num == seq+1), exactly
    // as ingest records them.
    s.state.messageRefs = {
        byRaw: { rm1: "m00001", rm2: "m00002", rm3: "m00003" },
        byRef: { m00001: "rm1", m00002: "rm2", m00003: "rm3" },
    };
    s.lastMessages = [
        { id: "rm1", role: "user", contentType: "text", text: "alpha lead-in" },
        { id: "rm2", role: "assistant", contentType: "text", text: "beta stored reply" },
        { id: "rm3", role: "user", contentType: "text", text: "gamma follow-up" },
    ];
    s.blockContents = new LazyBlockContents("restamp-msgs", {}, () => null);

    const dbPath = path.join(process.env.BILI_SESSIONS_DIR!, "index.db");
    const seed = UnifiedStore.open(dbPath);
    try {
        seed.ingestSession(planIngest(toLegacyLike(s)));
    } finally {
        seed.close();
    }

    // Cold-load restamp (#5196 slot retirement): two movers vacated their slots so
    // rm2/rm3 were re-parked onto FRESH HIGH numbers while their store rows keep
    // ref_num 2/3 frozen at index time. The live ledger now disagrees with the
    // store on every parked row — the exact divergence #6029 reports (~37% of a
    // long session's block-detail leaves read as "gone").
    s.state.messageRefs = {
        byRaw: { rm1: "m00001", rm2: "m01503", rm3: "m01504", rm4: "m01505" },
        byRef: { m00001: "rm1", m01503: "rm2", m01504: "rm3", m01505: "rm4" },
    };
    block.directMessageIds.push("rm4");
    block.effectiveMessageIds.push("rm4");
    s.lastMessages = []; // everything now needs storage hydration
    _resetDiskCacheForTest();
    try {
        const d = await buildSessionDetail("restamp-msgs");
        assert.ok(d);
        // Parked refs m01503/m01504 must hydrate by their STABLE msg_id even
        // though the store's ref_num column still says 2/3. Old code (ref-number
        // join) reported both missing. m01505 was never stored → honest miss.
        assert.deepEqual(d.blockDetails[0].directMessages, [
            { ref: "m00001", role: "user", preview: "alpha lead-in" },
            { ref: "m01503", role: "assistant", preview: "beta stored reply" },
            { ref: "m01504", role: "user", preview: "gamma follow-up" },
            { ref: "m01505", missing: true },
        ]);
        assert.equal(d.blockDetails[0].directMessageCount, 4);
        assert.deepEqual(d.blockDetails[0].directMessageRefs, ["m00001", "m01503", "m01504", "m01505"]);

        const parked = await buildMessageContent("restamp-msgs", "m01503");
        assert.ok(parked.ok, "click-to-open serves a restamped message from storage by msg_id");
        assert.equal(parked.role, "assistant");
        assert.equal(parked.text, "beta stored reply");

        assert.deepEqual(await buildMessageContent("restamp-msgs", "m01505"), { ok: false, reason: "unknown-message" });
    } finally {
        _resetDiskCacheForTest();
    }
});
