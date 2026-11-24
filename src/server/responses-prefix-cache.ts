import { createHash } from "node:crypto";
import type { BiliMessage, ResponseInputItem, ResponsesProjection, ResponsesRequestBody } from "acp-kernel/wire";
import { remapResponsesPartBindingSourceId, responsesToCoreWithToolImages } from "../responses-tool-output.js";
import { markerArtifactCount, stripRawStage, type RawStripCounts } from "../marker/engine.js";

type ResponseLayoutSlot = ResponsesProjection["layout"][number];

const CACHE_TTL_MS = 1_800_000; // 30 min (matches activeResponsesHints)
const MAX_CACHE_ENTRIES = 4;
const SAMPLE_BYTES = 32;
const SENTINEL_USER_TEXT = "__bili_responses_prefix_sentinel_v1__";
const SENTINEL_USER_ITEM: ResponseInputItem = {
    type: "message",
    role: "user",
    content: SENTINEL_USER_TEXT,
};

export interface CanonicalSysDevEntry {
    role: string;
    text: string;
}

export interface ResponsesPrefixEntry {
    sessionId: string;
    prefixByteLen: number;
    prefixHash: string;
    headSample: Buffer;
    tailSample: Buffer;
    prefixRawWireCount: number;
    prefixRawTextComparable: boolean;
    prefixMayCarryImages: boolean;
    prefixPostStripInput: ResponseInputItem[];
    prefixTypedItems: number;
    prefixDroppedEmpty: number;
    prefixMarkerCounts: RawStripCounts;
    prefixMsgs: BiliMessage[];
    prefixLayout: ResponseLayoutSlot[];
    prefixInputSystemParts: string[];
    prefixPreamble: ResponseInputItem[];
    prefixCustomToolCallIds: Set<string>;
    prefixDroppedReasoning: number;
    prefixHeadRole: "system" | "developer" | undefined;
    prefixBaseIdCounts: Map<string, number>;
    prefixToolNames: Map<string, string>;
    prefixCanonicalSysDev: CanonicalSysDevEntry[];
    dropAllReasoning: boolean;
    idSaltProbe: string;
    timer: ReturnType<typeof setTimeout>;
}

export interface ResponsesPrefixFastpathHit {
    entry: ResponsesPrefixEntry;
    envelope: Record<string, unknown>;
    tailItems: unknown[];
    envelopeJsonText: string;
    tailJsonText: string;
    nextPrefixByteLen: number;
    nextPrefixHash: string;
    nextHeadSample: Buffer;
    nextTailSample: Buffer;
    sha256Ms: number;
    parseMs: number;
}

export interface ResponsesPrefixSeedContext {
    bodyBuffer: Buffer;
    rawWireCount: number;
    rawTextComparable: boolean;
    bodyMayCarryImages: boolean;
}

export interface IncrementalProjectionResult {
    projection: ResponsesProjection;
    tailMsgs: BiliMessage[];
    tailToolNames: Array<[string, string]>;
    tailCanonicalSysDev: CanonicalSysDevEntry[];
    projMs: number;
}

const cacheEntries: ResponsesPrefixEntry[] = [];
const stats = { hits: 0, misses: 0, seeds: 0, incrementalFallbacks: 0 };

export function isResponsesPrefixCacheEnabled(): boolean {
    return (process.env.BILI_RESPONSES_PREFIX_CACHE ?? "").trim() !== "0";
}

export function clearResponsesPrefixCache(): void {
    for (const entry of cacheEntries) clearTimeout(entry.timer);
    cacheEntries.length = 0;
    stats.hits = 0;
    stats.misses = 0;
    stats.seeds = 0;
    stats.incrementalFallbacks = 0;
}

export function evictResponsesPrefixEntry(sessionId: string): void {
    const idx = cacheEntries.findIndex((e) => e.sessionId === sessionId);
    if (idx !== -1) {
        clearTimeout(cacheEntries[idx]!.timer);
        cacheEntries.splice(idx, 1);
    }
}

export function getResponsesPrefixCacheStats(): Readonly<typeof stats> & { entries: number } {
    return { ...stats, entries: cacheEntries.length };
}

function shouldDropAllReasoningEnv(): boolean {
    return (process.env.ACP_REASONING_KEEP ?? "").trim().toLowerCase() === "none";
}

function currentIdSaltProbe(): string {
    const probe = responsesToCoreWithToolImages({
        input: [{ type: "message", role: "user", content: "__bili_salt_probe__" }],
    });
    return probe.msgs[0]?.id ?? "";
}

function baseIdOf(id: string): string {
    // Kernel canonical IDs are `h_<16hex>` (no second `_`), and ClusterCounter
    // appends `_${n}` for duplicate occurrences (kernel/src/wire/message-id.ts).
    const secondUnderscore = id.indexOf("_", 2);
    return secondUnderscore === -1 ? id : id.slice(0, secondUnderscore);
}

function isAsciiWs(b: number): boolean {
    return b === 0x20 || b === 0x09 || b === 0x0A || b === 0x0D;
}

function skipWs(buf: Buffer, pos: number): number {
    const len = buf.length;
    while (pos < len && isAsciiWs(buf[pos]!)) pos++;
    return pos;
}

function trimTrailingWs(buf: Buffer, start: number, end: number): number {
    while (end > start && isAsciiWs(buf[end - 1]!)) end--;
    return end;
}

/** Skip a JSON string starting at opening quote `buf[start] === 0x22`.
 *  Uses SIMD-accelerated `Buffer.indexOf(0x22, ...)` so large text payloads
 *  scan at native C++ speed. Returns index immediately after closing quote, or -1. */
function skipJsonString(buf: Buffer, start: number): number {
    let pos = start + 1;
    const len = buf.length;
    while (pos < len) {
        const quote = buf.indexOf(0x22, pos);
        if (quote === -1) return -1;
        let backslashes = 0;
        for (let k = quote - 1; k >= start + 1 && buf[k] === 0x5C; k--) {
            backslashes++;
        }
        if ((backslashes & 1) === 0) return quote + 1;
        pos = quote + 1;
    }
    return -1;
}

/** Skip one complete RFC 8259 JSON value starting at `pos` (after whitespace).
 *  Iterative (zero recursion depth limit). Returns index right after value, or -1. */
function skipJsonValue(buf: Buffer, start: number): number {
    const len = buf.length;
    if (start >= len) return -1;
    const first = buf[start]!;
    if (first === 0x22) return skipJsonString(buf, start);
    if (first === 0x7B || first === 0x5B) {
        let depth = 1;
        let pos = start + 1;
        while (pos < len) {
            const b = buf[pos]!;
            if (b === 0x22) {
                pos = skipJsonString(buf, pos);
                if (pos === -1) return -1;
                continue;
            }
            if (b === 0x7B || b === 0x5B) {
                depth++;
            } else if (b === 0x7D || b === 0x5D) {
                depth--;
                if (depth === 0) return pos + 1;
            }
            pos++;
        }
        return -1;
    }
    if (first === 0x74) { // true
        return start + 4 <= len && buf[start + 1] === 0x72 && buf[start + 2] === 0x75 && buf[start + 3] === 0x65 ? start + 4 : -1;
    }
    if (first === 0x66) { // false
        return start + 5 <= len && buf[start + 1] === 0x61 && buf[start + 2] === 0x6C && buf[start + 3] === 0x73 && buf[start + 4] === 0x65 ? start + 5 : -1;
    }
    if (first === 0x6E) { // null
        return start + 4 <= len && buf[start + 1] === 0x75 && buf[start + 2] === 0x6C && buf[start + 3] === 0x6C ? start + 4 : -1;
    }
    if (first === 0x2D || (first >= 0x30 && first <= 0x39)) {
        let pos = start + 1;
        while (pos < len) {
            const b = buf[pos]!;
            if ((b >= 0x30 && b <= 0x39) || b === 0x2E || b === 0x65 || b === 0x45 || b === 0x2B || b === 0x2D) {
                pos++;
            } else {
                break;
            }
        }
        return pos;
    }
    return -1;
}

/** Locate top-level `"input":[` inside a JSON object buffer WITHOUT scanning
 *  the `"input"` array body itself. Stops the instant `"input":[` is found. */
export function findTopLevelInputArray(buf: Buffer): { openBracket: number; firstElemPos: number } | null {
    const len = buf.length;
    let pos = skipWs(buf, 0);
    if (pos >= len || buf[pos] !== 0x7B) return null; // '{'
    pos = skipWs(buf, pos + 1);
    if (pos >= len || buf[pos] === 0x7D) return null; // empty '{}'

    while (pos < len) {
        if (buf[pos] !== 0x22) return null; // key must start with '"'
        const keyStart = pos + 1;
        const keyEndQuote = skipJsonString(buf, pos);
        if (keyEndQuote === -1) return null;
        const keyLen = keyEndQuote - 1 - keyStart;
        const isInputKey =
            keyLen === 5 &&
            buf[keyStart] === 0x69 &&     // i
            buf[keyStart + 1] === 0x6E && // n
            buf[keyStart + 2] === 0x70 && // p
            buf[keyStart + 3] === 0x75 && // u
            buf[keyStart + 4] === 0x74;   // t

        pos = skipWs(buf, keyEndQuote);
        if (pos >= len || buf[pos] !== 0x3A) return null; // ':'
        pos = skipWs(buf, pos + 1);
        if (pos >= len) return null;

        if (isInputKey) {
            if (buf[pos] !== 0x5B) return null; // input must be an array
            const openBracket = pos;
            const firstElemPos = skipWs(buf, openBracket + 1);
            return { openBracket, firstElemPos };
        }

        // Skip non-input top-level property value.
        pos = skipJsonValue(buf, pos);
        if (pos === -1) return null;
        pos = skipWs(buf, pos);
        if (pos >= len) return null;
        if (buf[pos] === 0x2C) { // ','
            pos = skipWs(buf, pos + 1);
            continue;
        }
        if (buf[pos] === 0x7D) return null; // '}' reached without finding "input"
        return null;
    }
    return null;
}

/** Locate the full byte span of `"input":[ ... ]` on a cold seed turn. */
export function scanInputArraySpan(buf: Buffer): { openBracket: number; firstElemPos: number; trimmedEnd: number; closeBracket: number } | null {
    const loc = findTopLevelInputArray(buf);
    if (!loc) return null;
    const arrayEnd = skipJsonValue(buf, loc.openBracket);
    if (arrayEnd === -1 || buf[arrayEnd - 1] !== 0x5D) return null;
    const closeBracket = arrayEnd - 1;
    if (loc.firstElemPos >= closeBracket) return null; // empty array []
    const trimmedEnd = trimTrailingWs(buf, loc.firstElemPos, closeBracket);
    if (trimmedEnd <= loc.firstElemPos) return null;
    return {
        openBracket: loc.openBracket,
        firstElemPos: loc.firstElemPos,
        trimmedEnd,
        closeBracket,
    };
}

function copySample(buf: Buffer, start: number, end: number): Buffer {
    return Buffer.from(buf.subarray(start, end));
}

function makeSamples(buf: Buffer, start: number, end: number): { headSample: Buffer; tailSample: Buffer } {
    const len = end - start;
    const sampleLen = Math.min(SAMPLE_BYTES, len);
    return {
        headSample: copySample(buf, start, start + sampleLen),
        tailSample: copySample(buf, end - sampleLen, end),
    };
}

/** Attempt to match `buf` against a cached Responses prefix BEFORE running
 *  `JSON.parse(bodyBuffer)` on the full request.
 *
 *  Zero-trust-hole verification: every byte of `[firstElemPos, firstElemPos + prefixByteLen)`
 *  is verified by SHA-256 against the cached prefix digest. Any 1-byte edit
 *  anywhere in the prefix misses cleanly and falls back to normal `JSON.parse`. */
export function tryMatchResponsesPrefix(buf: Buffer, hintSessionId?: string): ResponsesPrefixFastpathHit | null {
    if (!isResponsesPrefixCacheEnabled() || cacheEntries.length === 0) return null;

    const loc = findTopLevelInputArray(buf);
    if (!loc) {
        stats.misses++;
        return null;
    }
    const { openBracket, firstElemPos } = loc;

    // Order candidates: hinted session first, then MRU order.
    const candidates: ResponsesPrefixEntry[] = [];
    if (hintSessionId) {
        const hinted = cacheEntries.find((e) => e.sessionId === hintSessionId);
        if (hinted) candidates.push(hinted);
    }
    for (let i = cacheEntries.length - 1; i >= 0; i--) {
        const e = cacheEntries[i]!;
        if (!candidates.includes(e)) candidates.push(e);
    }

    for (const entry of candidates) {
        const prefixEnd = firstElemPos + entry.prefixByteLen;
        if (prefixEnd >= buf.length) continue;

        const delimPos = skipWs(buf, prefixEnd);
        if (delimPos >= buf.length) continue;
        const delim = buf[delimPos]!;
        if (delim !== 0x2C && delim !== 0x5D) continue; // must be ',' or ']'

        // O(1) head+tail sample pre-filter before running native SHA-256.
        const sampleLen = entry.headSample.length;
        if (buf.subarray(firstElemPos, firstElemPos + sampleLen).compare(entry.headSample) !== 0) continue;
        if (buf.subarray(prefixEnd - sampleLen, prefixEnd).compare(entry.tailSample) !== 0) continue;

        const tHash0 = performance.now();
        const hasher = createHash("sha256");
        hasher.update(buf.subarray(firstElemPos, prefixEnd));
        const matchedDigest = hasher.copy().digest("hex");
        const sha256Ms = Number((performance.now() - tHash0).toFixed(1));
        if (matchedDigest !== entry.prefixHash) continue;

        // Prefix bytes are cryptographically identical! Now scan ONLY the tail.
        let closeBracket = -1;
        let trimmedEnd = prefixEnd;
        if (delim === 0x5D) {
            closeBracket = delimPos;
        } else {
            // Walk only the appended tail items after ',' to the closing ']'.
            let cur = skipWs(buf, delimPos + 1);
            if (cur >= buf.length || buf[cur] === 0x5D) return null; // trailing comma invalid JSON
            while (cur < buf.length) {
                const valEnd = skipJsonValue(buf, cur);
                if (valEnd === -1) return null;
                const next = skipWs(buf, valEnd);
                if (next >= buf.length) return null;
                if (buf[next] === 0x2C) {
                    cur = skipWs(buf, next + 1);
                    if (cur >= buf.length || buf[cur] === 0x5D) return null;
                    continue;
                }
                if (buf[next] === 0x5D) {
                    closeBracket = next;
                    trimmedEnd = trimTrailingWs(buf, prefixEnd, closeBracket);
                    break;
                }
                return null;
            }
            if (closeBracket === -1) return null;
        }

        // Extend the SAME SHA-256 state across the tiny tail so Turn N+2 has
        // its prefix hash ready with zero second pass!
        if (trimmedEnd > prefixEnd) {
            hasher.update(buf.subarray(prefixEnd, trimmedEnd));
        }
        const nextPrefixHash = hasher.digest("hex");
        const nextPrefixByteLen = trimmedEnd - firstElemPos;

        const tParse0 = performance.now();
        let envelope: Record<string, unknown>;
        let envelopeJsonText: string;
        let tailItems: unknown[];
        let tailJsonText: string;
        try {
            envelopeJsonText = buf.subarray(0, openBracket + 1).toString("utf8") + buf.subarray(closeBracket).toString("utf8");
            const parsedEnv = JSON.parse(envelopeJsonText) as unknown;
            if (!parsedEnv || typeof parsedEnv !== "object" || Array.isArray(parsedEnv)) return null;
            const envObj = parsedEnv as Record<string, unknown>;
            if (!Array.isArray(envObj.input) || envObj.input.length !== 0) return null;
            envelope = envObj;

            if (delim === 0x5D) {
                tailItems = [];
                tailJsonText = "[]";
            } else {
                tailJsonText = "[" + buf.subarray(delimPos + 1, closeBracket).toString("utf8") + "]";
                const parsedTail = JSON.parse(tailJsonText) as unknown;
                if (!Array.isArray(parsedTail)) return null;
                tailItems = parsedTail;
            }
        } catch {
            stats.misses++;
            return null;
        }
        const parseMs = Number((performance.now() - tParse0).toFixed(1));

        const nextSamples = makeSamples(buf, firstElemPos, trimmedEnd);
        stats.hits++;
        return {
            entry,
            envelope,
            tailItems,
            envelopeJsonText,
            tailJsonText,
            nextPrefixByteLen,
            nextPrefixHash,
            nextHeadSample: nextSamples.headSample,
            nextTailSample: nextSamples.tailSample,
            sha256Ms,
            parseMs,
        };
    }

    stats.misses++;
    return null;
}

function canonicalPartText(content: unknown): string {
    if (typeof content === "string") return content;
    if (!Array.isArray(content)) return "";
    return content.map((part) => {
        const p = part as { type?: unknown; text?: unknown };
        return (p.type === "input_text" || p.type === "output_text") && typeof p.text === "string" ? p.text : "";
    }).join("\n");
}

export function extractInputCanonicalSysDev(items: readonly unknown[]): CanonicalSysDevEntry[] {
    const out: CanonicalSysDevEntry[] = [];
    for (const rawItem of items) {
        if (!rawItem || typeof rawItem !== "object") continue;
        const item = rawItem as { type?: unknown; role?: unknown; content?: unknown };
        if ((item.type === undefined || item.type === "message") && (item.role === "system" || item.role === "developer")) {
            out.push({ role: String(item.role), text: canonicalPartText(item.content) });
        }
    }
    return out;
}

export function addRawStripCounts(a: RawStripCounts, b: RawStripCounts): RawStripCounts {
    return {
        panels: a.panels + b.panels,
        statusBodies: a.statusBodies + b.statusBodies,
        statusBodyLines: a.statusBodyLines + b.statusBodyLines,
        markerLines: a.markerLines + b.markerLines,
        nudges: a.nudges + b.nudges,
        carriers: a.carriers + b.carriers,
        strippedMs: b.strippedMs,
    };
}

/** Check whether `items` has exited the leading system/developer head
 *  (exact predicate of kernel/src/wire/responses.ts:783-792). */
function hasExitedSysDevHead(items: readonly ResponseInputItem[]): boolean {
    for (const item of items) {
        const role = (item as { role?: unknown }).role;
        const isSysDevMsgItem =
            (item.type === undefined || item.type === "message") &&
            (role === "system" || role === "developer");
        if (
            !isSysDevMsgItem &&
            item.type !== "additional_tools" &&
            item.type !== "mcp_list_tools"
        ) {
            return true;
        }
    }
    return false;
}

/** #2815 guard: `stripRawStage` protects the latest user turn from status-report
 *  stripping (`latestUserTurn: true`). Verify that the last user message in `items`
 *  carries zero strippable markers even when treated as a non-latest turn — if it
 *  does carry pasted ACP report text, skip seeding this turn so the next turn runs
 *  the full legacy strip pass and strips it cleanly once it ceases to be latest. */
function lastUserTurnIsStripInvariant(items: readonly ResponseInputItem[]): boolean {
    let lastUser: ResponseInputItem | undefined;
    for (let i = items.length - 1; i >= 0; i--) {
        const r = items[i] as { type?: unknown; role?: unknown } | undefined;
        if (r && typeof r === "object" && r.role === "user" && (r.type === "message" || r.type === undefined)) {
            lastUser = items[i];
            break;
        }
    }
    if (!lastUser) return true;
    // Clone only this single message (not the 38K array) and append a dummy
    // trailing user message so `lastUser` is evaluated with latestUserTurn=false.
    let cloned: unknown;
    try {
        cloned = structuredClone(lastUser);
    } catch {
        return false;
    }
    const probeCounts = stripRawStage({
        input: [cloned, { type: "message", role: "user", content: "__probe__" }],
    }, "responses");
    return markerArtifactCount(probeCounts) === 0;
}

function collectToolNamesFromItems(items: readonly ResponseInputItem[]): Array<[string, string]> {
    const out: Array<[string, string]> = [];
    for (const item of items) {
        if (!item || typeof item !== "object") continue;
        if (item.type === "function_call") {
            const fc = item as { call_id?: unknown; name?: unknown };
            if (typeof fc.call_id === "string" && typeof fc.name === "string") {
                out.push([fc.call_id, fc.name]);
            }
        } else if (item.type === "custom_tool_call") {
            const ctc = item as { call_id?: unknown; name?: unknown };
            if (typeof ctc.call_id === "string") {
                out.push([ctc.call_id, typeof ctc.name === "string" ? ctc.name : "custom"]);
            }
        }
    }
    return out;
}

function tailContainsSentinelOrIdxDependentCall(tailItems: readonly ResponseInputItem[]): boolean {
    for (const item of tailItems) {
        if (!item || typeof item !== "object") continue;
        if ((item.type === "custom_tool_call" || item.type === "custom_tool_call_output") && !(item as { call_id?: unknown }).call_id) {
            return true; // kernel falls back to `call_${idx}` which depends on full-array index
        }
        if ((item.type === "message" || item.type === undefined) && (item as { role?: unknown }).role === "user") {
            const c = (item as { content?: unknown }).content;
            if (c === SENTINEL_USER_TEXT) return true;
            if (Array.isArray(c) && c.some((p) => p && typeof p === "object" && (p as { text?: unknown }).text === SENTINEL_USER_TEXT)) {
                return true;
            }
        }
    }
    return false;
}

/** Build a ResponsesProjection incrementally from a verified prefix entry +
 *  ONLY the post-strip tail items. Returns `null` on any invariant mismatch
 *  so the caller falls back seamlessly to full `responsesToCore(parsed)`. */
export function buildIncrementalResponsesProjection(
    entry: ResponsesPrefixEntry,
    parsed: ResponsesRequestBody,
    tailPostStripItems: readonly ResponseInputItem[],
): IncrementalProjectionResult | null {
    if (entry.dropAllReasoning !== shouldDropAllReasoningEnv()) {
        stats.incrementalFallbacks++;
        return null;
    }
    if (entry.idSaltProbe !== currentIdSaltProbe()) {
        stats.incrementalFallbacks++;
        return null;
    }
    if (tailContainsSentinelOrIdxDependentCall(tailPostStripItems)) {
        stats.incrementalFallbacks++;
        return null;
    }

    const t0 = performance.now();

    // Determine tool-name stubs needed for any tail tool output whose paired
    // call lives in the prefix (so `toolNames.get(call_id)` matches full pass).
    const tailDefinedCallIds = new Set<string>();
    const tailToolNames: Array<[string, string]> = [];
    const neededStubIds = new Set<string>();
    const toolStubs: ResponseInputItem[] = [];

    for (const item of tailPostStripItems) {
        if (!item || typeof item !== "object") continue;
        if (item.type === "function_call") {
            const fc = item as { call_id?: unknown; name?: unknown };
            if (typeof fc.call_id === "string") {
                tailDefinedCallIds.add(fc.call_id);
                if (typeof fc.name === "string") tailToolNames.push([fc.call_id, fc.name]);
            }
        } else if (item.type === "custom_tool_call") {
            const ctc = item as { call_id?: unknown; name?: unknown };
            if (typeof ctc.call_id === "string") {
                tailDefinedCallIds.add(ctc.call_id);
                tailToolNames.push([ctc.call_id, typeof ctc.name === "string" ? ctc.name : "custom"]);
            }
        } else if (item.type === "function_call_output" || item.type === "custom_tool_call_output") {
            const out = item as { call_id?: unknown };
            if (typeof out.call_id === "string" && !tailDefinedCallIds.has(out.call_id) && !neededStubIds.has(out.call_id)) {
                const name = entry.prefixToolNames.get(out.call_id);
                if (name !== undefined) {
                    neededStubIds.add(out.call_id);
                    toolStubs.push({
                        type: "function_call",
                        call_id: out.call_id,
                        name,
                        // Unique uncollidable args so ClusterCounter in the mini-run
                        // can never collide with a real function_call in tailItems.
                        arguments: JSON.stringify({ __bili_prefix_stub__: `${toolStubs.length}:${out.call_id}` }),
                    });
                }
            }
        }
    }

    let tailMsgs: BiliMessage[] = [];
    let tailLayout: ResponseLayoutSlot[] = [];
    let tailPreamble: ResponseInputItem[] = [];
    let tailCustomToolCallIds: Set<string> = new Set();
    let tailDroppedReasoning = 0;

    if (tailPostStripItems.length > 0) {
        const miniInput: ResponseInputItem[] = [SENTINEL_USER_ITEM, ...toolStubs, ...tailPostStripItems];
        const miniProj = responsesToCoreWithToolImages({ input: miniInput });
        const skipCount = 1 + toolStubs.length;

        // Sanity invariant: sentinel + each function_call stub produces exactly 1 msg & 1 layout slot.
        if (miniProj.layout.length !== miniInput.length || miniProj.msgs.length < skipCount) {
            stats.incrementalFallbacks++;
            return null;
        }
        for (let i = 0; i < skipCount; i++) {
            if (miniProj.layout[i]?.original !== miniInput[i]) {
                stats.incrementalFallbacks++;
                return null;
            }
        }

        tailMsgs = miniProj.msgs.slice(skipCount);
        tailLayout = miniProj.layout.slice(skipCount);
        tailPreamble = miniProj.preamble;
        tailCustomToolCallIds = miniProj.customToolCallIds;
        tailDroppedReasoning = miniProj.droppedReasoning;

        // Fix wireIndex to reflect true position in full `parsed.input`.
        const wireOffset = entry.prefixPostStripInput.length - skipCount;
        for (const msg of tailMsgs) {
            if (typeof msg.wireIndex === "number") {
                msg.wireIndex += wireOffset;
            }
        }

        // Remap ClusterCounter IDs for any baseId that already appeared in the prefix.
        const remap = new Map<string, string>();
        const tailBaseSeen = new Map<string, number>();
        for (const msg of tailMsgs) {
            const base = baseIdOf(msg.id);
            const prefixCount = entry.prefixBaseIdCounts.get(base) ?? 0;
            if (prefixCount > 0) {
                const k = tailBaseSeen.get(base) ?? 0;
                tailBaseSeen.set(base, k + 1);
                const newId = `${base}_${prefixCount + k}`;
                remap.set(msg.id, newId);
                msg.id = newId;
                remapResponsesPartBindingSourceId(msg, newId);
            }
        }
        if (remap.size > 0) {
            for (const slot of tailLayout) {
                if (slot.coreId !== undefined) {
                    const mapped = remap.get(slot.coreId);
                    if (mapped !== undefined) slot.coreId = mapped;
                }
                if (slot.coreIds !== undefined) {
                    slot.coreIds = slot.coreIds.map((id) => remap.get(id) ?? id);
                }
            }
        }
    }

    const systemParts: string[] = [];
    if (typeof parsed.instructions === "string" && parsed.instructions.trim()) {
        systemParts.push(parsed.instructions);
    }
    if (entry.prefixInputSystemParts.length > 0) {
        systemParts.push(...entry.prefixInputSystemParts);
    }

    const customToolCallIds = new Set<string>(entry.prefixCustomToolCallIds);
    for (const id of tailCustomToolCallIds) customToolCallIds.add(id);

    const projection: ResponsesProjection = {
        msgs: tailMsgs.length > 0 ? entry.prefixMsgs.concat(tailMsgs) : entry.prefixMsgs.slice(),
        systemParts,
        preamble: tailPreamble.length > 0 ? entry.prefixPreamble.concat(tailPreamble) : entry.prefixPreamble.slice(),
        customToolCallIds,
        layout: tailLayout.length > 0 ? entry.prefixLayout.concat(tailLayout) : entry.prefixLayout.slice(),
        droppedReasoning: entry.prefixDroppedReasoning + tailDroppedReasoning,
        ...(entry.prefixHeadRole ? { headRole: entry.prefixHeadRole } : {}),
    };

    const tailCanonicalSysDev = extractInputCanonicalSysDev(tailPostStripItems);
    const projMs = Number((performance.now() - t0).toFixed(1));
    return { projection, tailMsgs, tailToolNames, tailCanonicalSysDev, projMs };
}

function storeEntry(entry: ResponsesPrefixEntry): void {
    // Evict existing entry for the same sessionId (if any) and enforce LRU cap.
    const existingIdx = cacheEntries.findIndex((e) => e.sessionId === entry.sessionId);
    if (existingIdx !== -1) {
        clearTimeout(cacheEntries[existingIdx]!.timer);
        cacheEntries.splice(existingIdx, 1);
    }
    while (cacheEntries.length >= MAX_CACHE_ENTRIES) {
        const evicted = cacheEntries.shift()!;
        clearTimeout(evicted.timer);
    }
    cacheEntries.push(entry);
}

function armTimer(sessionId: string, entryRef: { current?: ResponsesPrefixEntry }): ReturnType<typeof setTimeout> {
    const timer = setTimeout(() => {
        const idx = cacheEntries.findIndex((e) => e === entryRef.current);
        if (idx !== -1) cacheEntries.splice(idx, 1);
    }, CACHE_TTL_MS);
    timer.unref?.();
    return timer;
}

/** Commit an incrementally updated cache entry after a fastpath hit. */
export function commitIncrementalResponsesPrefixEntry(params: {
    sessionId: string;
    hit: ResponsesPrefixFastpathHit;
    nextPostStripInput: ResponseInputItem[];
    nextProjection: ResponsesProjection;
    tailMsgs: readonly BiliMessage[];
    tailToolNames: ReadonlyArray<[string, string]>;
    tailCanonicalSysDev: readonly CanonicalSysDevEntry[];
    nextTypedItems: number;
    nextDroppedEmpty: number;
    nextMarkerCounts: RawStripCounts;
    nextRawTextComparable: boolean;
    nextMayCarryImages: boolean;
}): boolean {
    if (!isResponsesPrefixCacheEnabled()) return false;
    const { hit, sessionId, nextPostStripInput, nextProjection } = params;
    if (nextPostStripInput.length === 0) return false;
    // Only re-check the latest user item if tail contained one.
    if (hit.tailItems.length > 0 && !lastUserTurnIsStripInvariant(nextPostStripInput)) return false;

    const prev = hit.entry;
    const sameSession = prev.sessionId === sessionId;

    // When extending the SAME session, update the maps in O(tail) instead of
    // cloning 38K-entry maps; when a forked child session hits a parent's prefix,
    // clone so the parent session's cache entry stays untouched!
    const nextBaseIdCounts = sameSession ? prev.prefixBaseIdCounts : new Map(prev.prefixBaseIdCounts);
    for (const msg of params.tailMsgs) {
        const base = baseIdOf(msg.id);
        nextBaseIdCounts.set(base, (nextBaseIdCounts.get(base) ?? 0) + 1);
    }

    const nextToolNames = sameSession ? prev.prefixToolNames : new Map(prev.prefixToolNames);
    for (const [callId, name] of params.tailToolNames) {
        nextToolNames.set(callId, name);
    }

    const entryRef: { current?: ResponsesPrefixEntry } = {};
    const entry: ResponsesPrefixEntry = {
        sessionId,
        prefixByteLen: hit.nextPrefixByteLen,
        prefixHash: hit.nextPrefixHash,
        headSample: hit.nextHeadSample,
        tailSample: hit.nextTailSample,
        prefixRawWireCount: prev.prefixRawWireCount + hit.tailItems.length,
        prefixRawTextComparable: params.nextRawTextComparable,
        prefixMayCarryImages: params.nextMayCarryImages,
        prefixPostStripInput: nextPostStripInput,
        prefixTypedItems: params.nextTypedItems,
        prefixDroppedEmpty: params.nextDroppedEmpty,
        prefixMarkerCounts: params.nextMarkerCounts,
        prefixMsgs: nextProjection.msgs,
        prefixLayout: nextProjection.layout,
        prefixInputSystemParts: prev.prefixInputSystemParts,
        prefixPreamble: nextProjection.preamble,
        prefixCustomToolCallIds: nextProjection.customToolCallIds,
        prefixDroppedReasoning: nextProjection.droppedReasoning,
        prefixHeadRole: prev.prefixHeadRole,
        prefixBaseIdCounts: nextBaseIdCounts,
        prefixToolNames: nextToolNames,
        prefixCanonicalSysDev: params.tailCanonicalSysDev.length > 0
            ? prev.prefixCanonicalSysDev.concat(params.tailCanonicalSysDev)
            : prev.prefixCanonicalSysDev,
        dropAllReasoning: prev.dropAllReasoning,
        idSaltProbe: prev.idSaltProbe,
        timer: armTimer(sessionId, entryRef),
    };
    entryRef.current = entry;
    storeEntry(entry);
    return true;
}

/** Seed the prefix cache on a cold turn (after full `responsesToCore` succeeded). */
export function seedResponsesPrefixCache(params: {
    sessionId: string;
    seed: ResponsesPrefixSeedContext;
    parsed: ResponsesRequestBody;
    postStripInput: ResponseInputItem[];
    projection: ResponsesProjection;
    typedItems: number;
    droppedEmpty: number;
    markerCounts: RawStripCounts;
}): { seeded: boolean; tookMs: number; bytes: number } {
    if (!isResponsesPrefixCacheEnabled()) return { seeded: false, tookMs: 0, bytes: 0 };
    const { sessionId, seed, parsed, postStripInput, projection } = params;
    if (postStripInput.length === 0) return { seeded: false, tookMs: 0, bytes: 0 };
    if (!hasExitedSysDevHead(postStripInput)) return { seeded: false, tookMs: 0, bytes: 0 };
    if (!lastUserTurnIsStripInvariant(postStripInput)) return { seeded: false, tookMs: 0, bytes: 0 };

    const t0 = performance.now();
    const span = scanInputArraySpan(seed.bodyBuffer);
    if (!span) return { seeded: false, tookMs: 0, bytes: 0 };

    const prefixSlice = seed.bodyBuffer.subarray(span.firstElemPos, span.trimmedEnd);
    const prefixHash = createHash("sha256").update(prefixSlice).digest("hex");
    const samples = makeSamples(seed.bodyBuffer, span.firstElemPos, span.trimmedEnd);

    const hasInstructions = typeof parsed.instructions === "string" && parsed.instructions.trim().length > 0;
    const prefixInputSystemParts = hasInstructions && projection.systemParts[0] === parsed.instructions
        ? projection.systemParts.slice(1)
        : projection.systemParts.slice();

    const prefixBaseIdCounts = new Map<string, number>();
    for (const msg of projection.msgs) {
        const base = baseIdOf(msg.id);
        prefixBaseIdCounts.set(base, (prefixBaseIdCounts.get(base) ?? 0) + 1);
    }

    const prefixToolNames = new Map<string, string>();
    for (const [callId, name] of collectToolNamesFromItems(postStripInput)) {
        prefixToolNames.set(callId, name);
    }

    const prefixCanonicalSysDev = extractInputCanonicalSysDev(postStripInput);

    const entryRef: { current?: ResponsesPrefixEntry } = {};
    const entry: ResponsesPrefixEntry = {
        sessionId,
        prefixByteLen: span.trimmedEnd - span.firstElemPos,
        prefixHash,
        headSample: samples.headSample,
        tailSample: samples.tailSample,
        prefixRawWireCount: seed.rawWireCount,
        prefixRawTextComparable: seed.rawTextComparable,
        prefixMayCarryImages: seed.bodyMayCarryImages,
        prefixPostStripInput: postStripInput.slice(),
        prefixTypedItems: params.typedItems,
        prefixDroppedEmpty: params.droppedEmpty,
        prefixMarkerCounts: { ...params.markerCounts },
        prefixMsgs: projection.msgs.slice(),
        prefixLayout: projection.layout.slice(),
        prefixInputSystemParts,
        prefixPreamble: projection.preamble.slice(),
        prefixCustomToolCallIds: new Set(projection.customToolCallIds),
        prefixDroppedReasoning: projection.droppedReasoning,
        prefixHeadRole: projection.headRole,
        prefixBaseIdCounts,
        prefixToolNames,
        prefixCanonicalSysDev,
        dropAllReasoning: shouldDropAllReasoningEnv(),
        idSaltProbe: currentIdSaltProbe(),
        timer: armTimer(sessionId, entryRef),
    };
    entryRef.current = entry;
    storeEntry(entry);
    stats.seeds++;
    return {
        seeded: true,
        tookMs: Number((performance.now() - t0).toFixed(1)),
        bytes: entry.prefixByteLen,
    };
}
