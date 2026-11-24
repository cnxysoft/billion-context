import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { defaultConfig } from "acp-kernel";
import type { ResponseInputItem, ResponsesProjection, ResponsesRequestBody } from "acp-kernel/wire";
import {
    buildIncrementalResponsesProjection,
    clearResponsesPrefixCache,
    commitIncrementalResponsesPrefixEntry,
    findTopLevelInputArray,
    getResponsesPrefixCacheStats,
    scanInputArraySpan,
    seedResponsesPrefixCache,
    tryMatchResponsesPrefix,
} from "../src/server/responses-prefix-cache.ts";
import { responsesToCoreWithToolImages } from "../src/responses-tool-output.ts";
import { stripRawStage } from "../src/marker/engine.ts";
import { startServer } from "../src/server.ts";
import type { ProxyOptions } from "../src/config.ts";
import { _setStoreForTest, SessionStore } from "../src/persist.ts";
import { _setForTest as setRegistryForTest } from "../src/registry.ts";
import { _resetSessionsForTest } from "../src/session.ts";

process.env.NODE_ENV = "test";
process.env.BILI_PERSIST = "0";

function snapshotProjection(p: ResponsesProjection) {
    return {
        msgs: p.msgs.map((m) => ({
            id: m.id,
            role: m.role,
            contentType: m.contentType,
            text: m.text,
            toolName: m.toolName,
            toolCallId: m.toolCallId,
            wireIndex: m.wireIndex,
            originalRole: m.originalRole,
            imagePart: m.imagePart,
            imageMediaType: m.imageMediaType,
            imageBase64: m.imageBase64,
        })),
        layout: p.layout.map((s) => ({
            coreId: s.coreId,
            coreIds: s.coreIds,
            original: s.original,
        })),
        systemParts: p.systemParts,
        preamble: p.preamble,
        customToolCallIds: [...p.customToolCallIds].sort(),
        droppedReasoning: p.droppedReasoning,
        headRole: p.headRole,
    };
}

const TINY_PNG_DATA_URL =
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";

beforeEach(() => {
    clearResponsesPrefixCache();
    delete process.env.BILI_RESPONSES_PREFIX_CACHE;
    delete process.env.ACP_REASONING_KEEP;
});

afterEach(() => {
    clearResponsesPrefixCache();
    delete process.env.BILI_RESPONSES_PREFIX_CACHE;
    delete process.env.ACP_REASONING_KEEP;
});

test("findTopLevelInputArray & scanInputArraySpan handle compact, pretty, escaped quotes, CJK, and decoy strings", () => {
    const body = {
        model: "gpt-5",
        instructions: "Decoy string containing \"input\":[1,2,3] and backslash \\\"input\\\":[4] and 中文🚀",
        metadata: { nested: { input: [{ fake: true }] }, arr: [1, true, false, null, -3.14e+2] },
        input: [
            { type: "message", role: "user", content: "你好，世界 \"quoted\" \\\\ end" },
            { type: "message", role: "assistant", content: "收到 🚀" },
        ],
        stream: true,
    };

    for (const space of [0, 2]) {
        const buf = Buffer.from(JSON.stringify(body, null, space), "utf8");
        const loc = findTopLevelInputArray(buf);
        assert.ok(loc, `space=${space}: should find top-level input array`);
        assert.equal(buf[loc.openBracket], 0x5B);

        const span = scanInputArraySpan(buf);
        assert.ok(span, `space=${space}: should find full input span`);
        assert.equal(buf[span.closeBracket], 0x5D);
        const reconstructed = JSON.parse("[" + buf.subarray(span.firstElemPos, span.trimmedEnd).toString("utf8") + "]");
        assert.deepEqual(reconstructed, body.input);
    }

    // Non-array string input -> null
    assert.equal(findTopLevelInputArray(Buffer.from(JSON.stringify({ model: "m", input: "hello" }))), null);
    // Empty array -> scanInputArraySpan returns null
    assert.equal(scanInputArraySpan(Buffer.from(JSON.stringify({ model: "m", input: [] }))), null);
});

test("tryMatchResponsesPrefix verifies SHA-256 bit-for-bit, detects 1-byte tamper, supports 0-delta & envelope changes", () => {
    const turn1Items: ResponseInputItem[] = [
        { type: "message", role: "developer", content: "Head dev prompt" },
        { type: "message", role: "user", content: "First user turn with enough text to exceed 32 bytes easily" },
        { type: "message", role: "assistant", content: "First assistant reply" },
    ];
    const turn1Body: ResponsesRequestBody = {
        model: "deepseek-chat",
        instructions: "System v1",
        input: turn1Items,
        stream: true,
    };
    const buf1 = Buffer.from(JSON.stringify(turn1Body), "utf8");
    const proj1 = responsesToCoreWithToolImages(turn1Body);
    const seedRes = seedResponsesPrefixCache({
        sessionId: "sess-1",
        seed: { bodyBuffer: buf1, rawWireCount: turn1Items.length, rawTextComparable: true, bodyMayCarryImages: false },
        parsed: turn1Body,
        postStripInput: turn1Items,
        projection: proj1,
        typedItems: 0,
        droppedEmpty: 0,
        markerCounts: stripRawStage({ input: [] }, "responses"),
    });
    assert.equal(seedRes.seeded, true);

    // 1. Warm append with CHANGED instructions + tools -> HIT!
    const turn2Tail: ResponseInputItem[] = [
        { type: "message", role: "user", content: "Second user turn" },
    ];
    const turn2Body: ResponsesRequestBody = {
        model: "deepseek-chat",
        instructions: "System v2 (updated!)",
        tools: [{ type: "function", name: "read_file" }],
        input: [...turn1Items, ...turn2Tail],
        stream: true,
    };
    const buf2 = Buffer.from(JSON.stringify(turn2Body), "utf8");
    const hit2 = tryMatchResponsesPrefix(buf2, "sess-1");
    assert.ok(hit2, "Turn 2 should hit prefix cache");
    assert.deepEqual(hit2.tailItems, turn2Tail);
    assert.equal(hit2.envelope.instructions, "System v2 (updated!)");
    assert.deepEqual(hit2.envelope.tools, turn2Body.tools);

    // 2. 1-byte tamper in the middle of item #1 -> MUST MISS!
    const tamperedItems = structuredClone(turn1Items);
    (tamperedItems[1] as { content: string }).content = "First user turn with enough text to exceed 32 bytes easilZ";
    const tamperedBuf = Buffer.from(JSON.stringify({ ...turn2Body, input: [...tamperedItems, ...turn2Tail] }), "utf8");
    assert.equal(tryMatchResponsesPrefix(tamperedBuf, "sess-1"), null, "1-byte edit inside prefix MUST miss SHA-256");

    // 3. 0-delta resend (identical input[]) -> HIT with empty tail!
    const hitZero = tryMatchResponsesPrefix(buf1, "sess-1");
    assert.ok(hitZero, "0-delta resend should hit");
    assert.deepEqual(hitZero.tailItems, []);
    assert.equal(hitZero.nextPrefixHash, hitZero.entry.prefixHash);
});

test("buildIncrementalResponsesProjection achieves 100% parity with full responsesToCore across tools, duplicates, mid-sys/dev, images, and reasoning", () => {
    const prefixItems: ResponseInputItem[] = [
        { type: "message", role: "developer", content: "Leading dev rule" },
        { type: "message", role: "system", content: "Leading sys rule" },
        { type: "message", role: "user", content: "继续" },
        {
            type: "reasoning",
            summary: [{ type: "summary_text", text: "Thinking step 1" }],
        } as unknown as ResponseInputItem,
        {
            type: "function_call",
            call_id: "call_prefix_1",
            name: "read_file",
            arguments: JSON.stringify({ path: "a.ts" }),
        } as unknown as ResponseInputItem,
        {
            type: "function_call_output",
            call_id: "call_prefix_1",
            output: "file contents a",
        } as unknown as ResponseInputItem,
        {
            type: "function_call",
            call_id: "call_prefix_pending",
            name: "bash",
            arguments: JSON.stringify({ cmd: "ls" }),
        } as unknown as ResponseInputItem,
        {
            type: "custom_tool_call",
            call_id: "ctc_1",
            name: "apply_patch",
            input: "*** Begin Patch",
        } as unknown as ResponseInputItem,
        { type: "message", role: "user", content: "继续" },
        { type: "message", role: "developer", content: "Mid-history developer notification #1" },
        { type: "message", role: "user", content: "继续" },
    ];

    const turn1Body: ResponsesRequestBody = {
        model: "gpt-5",
        instructions: "Top-level instructions v1",
        input: structuredClone(prefixItems),
    };
    const buf1 = Buffer.from(JSON.stringify(turn1Body), "utf8");
    const proj1 = responsesToCoreWithToolImages(turn1Body);
    const seedRes = seedResponsesPrefixCache({
        sessionId: "parity-sess",
        seed: { bodyBuffer: buf1, rawWireCount: prefixItems.length, rawTextComparable: true, bodyMayCarryImages: false },
        parsed: turn1Body,
        postStripInput: turn1Body.input as ResponseInputItem[],
        projection: proj1,
        typedItems: 0,
        droppedEmpty: 0,
        markerCounts: stripRawStage({ input: [] }, "responses"),
    });
    assert.equal(seedRes.seeded, true);

    // Turn 2 tail exercises EVERY tricky interaction:
    // - function_call_output referencing call_id from prefix (`call_prefix_pending` -> toolName="bash")
    // - custom_tool_call_output referencing prefix custom_tool_call (`ctc_1` -> toolName="apply_patch")
    // - TWO more duplicate "继续" user messages (prefix had 3 -> must become _3 and _4!)
    // - mid-history developer + system items (must stay positioned sidecar cores, NOT hoisted!)
    // - multi-part user message with text + image data URL
    // - reasoning item + new function_call + output
    const tail2Items: ResponseInputItem[] = [
        {
            type: "function_call_output",
            call_id: "call_prefix_pending",
            output: "README.md\nsrc/",
        } as unknown as ResponseInputItem,
        {
            type: "custom_tool_call_output",
            call_id: "ctc_1",
            output: "patched ok",
        } as unknown as ResponseInputItem,
        { type: "message", role: "user", content: "继续" },
        { type: "message", role: "developer", content: "Mid-history developer notification #2" },
        { type: "message", role: "system", content: "Mid-history system note" },
        {
            type: "message",
            role: "user",
            content: [
                { type: "input_text", text: "继续" },
                { type: "input_image", image_url: TINY_PNG_DATA_URL },
            ],
        } as unknown as ResponseInputItem,
        {
            type: "reasoning",
            summary: [{ type: "summary_text", text: "Thinking step 2" }],
        } as unknown as ResponseInputItem,
        {
            type: "function_call",
            call_id: "call_tail_2",
            name: "bash",
            arguments: JSON.stringify({ cmd: "pwd" }),
        } as unknown as ResponseInputItem,
        {
            type: "function_call_output",
            call_id: "call_tail_2",
            output: "/repo",
        } as unknown as ResponseInputItem,
    ];

    const turn2Body: ResponsesRequestBody = {
        model: "gpt-5",
        instructions: "Top-level instructions v2",
        input: [...prefixItems, ...tail2Items],
    };
    const buf2 = Buffer.from(JSON.stringify(turn2Body), "utf8");
    const hit2 = tryMatchResponsesPrefix(buf2, "parity-sess");
    assert.ok(hit2, "Turn 2 should hit");

    const parsed2: ResponsesRequestBody = {
        ...(hit2.envelope as ResponsesRequestBody),
        input: hit2.entry.prefixPostStripInput.concat(hit2.tailItems as ResponseInputItem[]),
    };
    const inc2 = buildIncrementalResponsesProjection(hit2.entry, parsed2, hit2.tailItems as ResponseInputItem[]);
    assert.ok(inc2, "Incremental projection should succeed");

    // Compare against ground-truth full responsesToCoreWithToolImages!
    const truth2 = responsesToCoreWithToolImages(structuredClone(turn2Body));
    assert.deepEqual(snapshotProjection(inc2.projection), snapshotProjection(truth2));

    // Commit Turn 2 and verify Turn 3 chains seamlessly!
    const committed = commitIncrementalResponsesPrefixEntry({
        sessionId: "parity-sess",
        hit: hit2,
        nextPostStripInput: parsed2.input as ResponseInputItem[],
        nextProjection: inc2.projection,
        tailMsgs: inc2.tailMsgs,
        tailToolNames: inc2.tailToolNames,
        tailCanonicalSysDev: inc2.tailCanonicalSysDev,
        nextTypedItems: 0,
        nextDroppedEmpty: 0,
        nextMarkerCounts: hit2.entry.prefixMarkerCounts,
        nextRawTextComparable: true,
        nextMayCarryImages: true,
    });
    assert.equal(committed, true);

    const tail3Items: ResponseInputItem[] = [
        { type: "message", role: "user", content: "继续" },
        { type: "message", role: "assistant", content: "Done!" },
    ];
    const turn3Body: ResponsesRequestBody = {
        model: "gpt-5",
        instructions: "Top-level instructions v2",
        input: [...prefixItems, ...tail2Items, ...tail3Items],
    };
    const buf3 = Buffer.from(JSON.stringify(turn3Body), "utf8");
    const hit3 = tryMatchResponsesPrefix(buf3, "parity-sess");
    assert.ok(hit3, "Turn 3 should chain-hit from incremental SHA-256 hash");
    const parsed3: ResponsesRequestBody = {
        ...(hit3.envelope as ResponsesRequestBody),
        input: hit3.entry.prefixPostStripInput.concat(hit3.tailItems as ResponseInputItem[]),
    };
    const inc3 = buildIncrementalResponsesProjection(hit3.entry, parsed3, hit3.tailItems as ResponseInputItem[]);
    assert.ok(inc3, "Turn 3 incremental projection should succeed");
    const truth3 = responsesToCoreWithToolImages(structuredClone(turn3Body));
    assert.deepEqual(snapshotProjection(inc3.projection), snapshotProjection(truth3));
});

test("edge cases: ACP_REASONING_KEEP toggle, kill-switch, missing custom_tool_call id, forked child isolation, and #2815 latest-user guard", () => {
    const items: ResponseInputItem[] = [
        { type: "message", role: "user", content: "Hello world message 1" },
        { type: "message", role: "assistant", content: "Reply 1" },
    ];
    const body1: ResponsesRequestBody = { model: "m", input: structuredClone(items) };
    const buf1 = Buffer.from(JSON.stringify(body1), "utf8");
    const proj1 = responsesToCoreWithToolImages(body1);
    const seed = seedResponsesPrefixCache({
        sessionId: "parent-sess",
        seed: { bodyBuffer: buf1, rawWireCount: items.length, rawTextComparable: true, bodyMayCarryImages: false },
        parsed: body1,
        postStripInput: body1.input as ResponseInputItem[],
        projection: proj1,
        typedItems: 0,
        droppedEmpty: 0,
        markerCounts: stripRawStage({ input: [] }, "responses"),
    });
    assert.equal(seed.seeded, true);

    // 1. ACP_REASONING_KEEP toggled -> buildIncrementalResponsesProjection returns null (safe fallback)
    const body2: ResponsesRequestBody = {
        model: "m",
        input: [...items, { type: "message", role: "user", content: "Hello world message 1" }],
    };
    const buf2 = Buffer.from(JSON.stringify(body2), "utf8");
    const hit2 = tryMatchResponsesPrefix(buf2, "parent-sess");
    assert.ok(hit2);
    process.env.ACP_REASONING_KEEP = "none";
    assert.equal(buildIncrementalResponsesProjection(hit2.entry, body2, hit2.tailItems as ResponseInputItem[]), null);
    delete process.env.ACP_REASONING_KEEP;

    // 2. custom_tool_call without call_id in tail -> safe fallback (returns null)
    const missingIdTail = [{ type: "custom_tool_call", name: "x", input: "{}" }] as unknown as ResponseInputItem[];
    assert.equal(buildIncrementalResponsesProjection(hit2.entry, body2, missingIdTail), null);

    // 3. Forked child session hitting parent's prefix does NOT mutate parent's baseIdCounts
    const parentCountBefore = hit2.entry.prefixBaseIdCounts.get(proj1.msgs[0]!.id);
    const childInc = buildIncrementalResponsesProjection(hit2.entry, body2, hit2.tailItems as ResponseInputItem[]);
    assert.ok(childInc);
    commitIncrementalResponsesPrefixEntry({
        sessionId: "child-fork-sess",
        hit: hit2,
        nextPostStripInput: [...items, ...(hit2.tailItems as ResponseInputItem[])],
        nextProjection: childInc.projection,
        tailMsgs: childInc.tailMsgs,
        tailToolNames: childInc.tailToolNames,
        tailCanonicalSysDev: childInc.tailCanonicalSysDev,
        nextTypedItems: 0,
        nextDroppedEmpty: 0,
        nextMarkerCounts: hit2.entry.prefixMarkerCounts,
        nextRawTextComparable: true,
        nextMayCarryImages: false,
    });
    assert.equal(hit2.entry.prefixBaseIdCounts.get(proj1.msgs[0]!.id), parentCountBefore, "parent session counts must remain untouched on fork hit");

    // 4. Kill-switch BILI_RESPONSES_PREFIX_CACHE=0 disables cache
    process.env.BILI_RESPONSES_PREFIX_CACHE = "0";
    assert.equal(tryMatchResponsesPrefix(buf2, "parent-sess"), null);
    delete process.env.BILI_RESPONSES_PREFIX_CACHE;

    // 5. #2815/#5812 guard: latest user turn carrying a strippable [ACP] marker line skips seeding
    clearResponsesPrefixCache();
    const statusReportItems: ResponseInputItem[] = [
        { type: "message", role: "user", content: "📦 [ACP] Context compressed (500 tokens saved)" },
    ];
    const reportBody: ResponsesRequestBody = { model: "m", input: structuredClone(statusReportItems) };
    const reportBuf = Buffer.from(JSON.stringify(reportBody), "utf8");
    const reportProj = responsesToCoreWithToolImages(reportBody);
    const reportSeed = seedResponsesPrefixCache({
        sessionId: "report-sess",
        seed: { bodyBuffer: reportBuf, rawWireCount: 1, rawTextComparable: true, bodyMayCarryImages: false },
        parsed: reportBody,
        postStripInput: reportBody.input as ResponseInputItem[],
        projection: reportProj,
        typedItems: 0,
        droppedEmpty: 0,
        markerCounts: stripRawStage({ input: [] }, "responses"),
    });
    assert.equal(reportSeed.seeded, false, "latest user turn with strippable marker must skip seeding (#2815)");
});

test("E2E HTTP proxy parity: 4-turn Responses session (seed -> hit -> hit -> tamper-miss) matches BILI_RESPONSES_PREFIX_CACHE=0 byte-for-byte", async () => {
    const okResponsesBody = JSON.stringify({
        id: "resp_test_1",
        object: "response",
        status: "completed",
        model: "deepseek-chat",
        output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "ok" }] }],
        usage: { input_tokens: 100, output_tokens: 5, total_tokens: 105 },
    });

    async function runFourTurns(cacheEnabled: boolean): Promise<{ forwarded: unknown[]; cacheStats: ReturnType<typeof getResponsesPrefixCacheStats> }> {
        clearResponsesPrefixCache();
        _resetSessionsForTest();
        if (cacheEnabled) delete process.env.BILI_RESPONSES_PREFIX_CACHE;
        else process.env.BILI_RESPONSES_PREFIX_CACHE = "0";

        const forwarded: unknown[] = [];
        const upstream = http.createServer((req, res) => {
            const chunks: Buffer[] = [];
            req.on("data", (c: Buffer) => chunks.push(c));
            req.on("end", () => {
                forwarded.push(JSON.parse(Buffer.concat(chunks).toString("utf8")));
                res.writeHead(200, { "content-type": "application/json" });
                res.end(okResponsesBody);
            });
        });
        upstream.listen(0, "127.0.0.1");
        await once(upstream, "listening");
        const upstreamPort = (upstream.address() as AddressInfo).port;

        _setStoreForTest(new SessionStore({ enabled: false }));
        setRegistryForTest({});
        const upstreamUrl = `http://127.0.0.1:${upstreamPort}`;
        const proxy = await startServer({
            port: 0,
            host: "127.0.0.1",
            upstream: upstreamUrl,
            routes: { [upstreamUrl]: {} },
            modelContextLimit: 200_000,
            kernelConfig: defaultConfig(200_000),
            compress: { injectTool: false, injectNudge: false },
            promptCache: { routing: "auto" },
            sessionHeader: "x-acp-session",
            log: false,
            debug: false,
            passthrough: false,
            autoUpdate: false,
            compat: { roles: {} },
            streamErrorShape: "protocol",
            passthroughSource: null,
            autoRestartOnUpdate: false,
            updateTag: "latest",
            advisoryCheck: false,
            releaseNotesCheck: false,
            mitm: { enabled: false, domains: [] },
        } as unknown as ProxyOptions);
        await once(proxy, "listening");
        const proxyPort = (proxy.address() as AddressInfo).port;

        const postTurn = async (body: Record<string, unknown>) => {
            const res = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses`, {
                method: "POST",
                headers: {
                    "content-type": "application/json",
                    "x-acp-session": "e2e-responses-prefix-sess",
                    "x-bili-plugin": "dsh",
                },
                body: JSON.stringify(body),
            });
            assert.equal(res.status, 200);
            await res.text();
        };

        try {
            const baseInput: ResponseInputItem[] = [
                { type: "message", role: "developer", content: "System policy" },
                { type: "message", role: "user", content: "继续" },
                {
                    type: "function_call",
                    call_id: "fc_1",
                    name: "read_file",
                    arguments: JSON.stringify({ path: "README.md" }),
                } as unknown as ResponseInputItem,
            ];
            // Turn 1: cold seed
            await postTurn({ model: "deepseek-chat", instructions: "Inst v1", input: baseInput, stream: false });

            // Turn 2: append function_call_output referencing Turn 1's fc_1 + assistant reply -> HIT
            const turn2Input: ResponseInputItem[] = [
                ...baseInput,
                { type: "function_call_output", call_id: "fc_1", output: "Hello README" } as unknown as ResponseInputItem,
                { type: "message", role: "assistant", content: "Read it." },
            ];
            await postTurn({ model: "deepseek-chat", instructions: "Inst v1", input: turn2Input, stream: false });

            // Turn 3: append repeated "继续" + mid-history developer + updated instructions -> HIT
            const turn3Input: ResponseInputItem[] = [
                ...turn2Input,
                { type: "message", role: "developer", content: "Mid-turn hook notice" },
                { type: "message", role: "user", content: "继续" },
            ];
            await postTurn({ model: "deepseek-chat", instructions: "Inst v2", input: turn3Input, stream: false });

            // Turn 4: 1-byte tamper in Turn 1 message -> MISS + clean fallback
            const tamperedInput = structuredClone(turn3Input);
            (tamperedInput[1] as { content: string }).content = "继续!";
            await postTurn({ model: "deepseek-chat", instructions: "Inst v2", input: tamperedInput, stream: false });

            return { forwarded, cacheStats: getResponsesPrefixCacheStats() };
        } finally {
            proxy.close();
            upstream.close();
            _resetSessionsForTest();
        }
    }

    const withCache = await runFourTurns(true);
    const withoutCache = await runFourTurns(false);

    assert.equal(withCache.cacheStats.seeds, 2, "Turn 1 seeds, Turn 4 tamper-miss re-seeds");
    assert.equal(withCache.cacheStats.hits, 2, "Turn 2 and Turn 3 must hit prefix cache");
    assert.equal(withCache.cacheStats.misses, 1, "Turn 4 1-byte tamper must miss prefix cache");
    assert.equal(withoutCache.cacheStats.hits, 0);
    assert.equal(withoutCache.cacheStats.seeds, 0);

    const normalizeTimestamps = (v: unknown) =>
        JSON.parse(JSON.stringify(v).replace(/time=\\"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\\"/g, 'time=\\"TS\\"'));
    assert.equal(withCache.forwarded.length, 4);
    assert.deepEqual(
        normalizeTimestamps(withCache.forwarded),
        normalizeTimestamps(withoutCache.forwarded),
        "Every single forwarded upstream body must be 100% identical with vs without prefix cache",
    );
});
