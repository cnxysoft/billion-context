// Chunk-boundary determinism fuzz for the tag-echo filter (#3198 round 2).
//
// The owner's ask: PROVE the tag in/out pipeline changes nothing across the
// filter — the streaming state machine (createTagEchoFilter) must produce
// byte-identical output for the SAME logical model output regardless of how
// the network splits it into chunks. SSE deltas arrive in tokenizer-sized
// fragments that vary between requests; if the filter's release/drop decisions
// depended on chunk boundaries, the same generation would persist differently
// on a retry, and the resent history would drift (a cache seam at exactly the
// message that changed).
//
// Method: a corpus of documented imitation shapes (each cited to the issue
// that added it) is fed through the REAL filter in many chunkings — one big
// push, fixed sizes 1/2/3/5/8/13/21/34, and 40 seeded random splittings — and
// every run's concatenated push+flush output must be byte-identical to the
// whole-buffer run. A second, informational check compares the streaming
// result with the whole-text stripper (stripAcpTags) to surface any
// streaming-vs-settled disagreement.
import test from "node:test";
import assert from "node:assert/strict";
import { composeStreamFilters, createBiliArtifactFilter, createDsmlLeakFilter, createLeadingNewlineFilter, createMarkerLineFilter, createTagEchoFilter, stripAcpTags, stripDsmlLeaks, stripResponsesText, trimLeadingNewlines } from "../src/loop/tag-echo-filter.ts";

function mulberry32(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
        a |= 0;
        a = (a + 0x6d2b79f5) | 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

/** Split `input` into chunks of a seeded random shape (sizes 1..40). */
function randomSplits(input: string, seed: number): number[] {
    const rnd = mulberry32(seed);
    const splits: number[] = [];
    let left = input.length;
    while (left > 0) {
        const n = Math.min(left, 1 + Math.floor(rnd() * 40));
        splits.push(n);
        left -= n;
    }
    return splits;
}

/** Feed `input` through a fresh filter in the given chunk sizes + flush. */
function runStream(input: string, splits: number[]): string {
    const f = createTagEchoFilter();
    let out = "";
    let pos = 0;
    for (const len of splits) {
        out += f.push(input.slice(pos, pos + len));
        pos += len;
    }
    if (pos < input.length) out += f.push(input.slice(pos));
    out += f.flush();
    return out;
}

// Corpus. \x3c/\x3e per repo convention (KDD #2). Each entry cites the shape's
// provenance issue from src/loop/tag-echo-filter.ts.
const CORPUS: Array<{ name: string; text: string }> = [
    { name: "plain pair #206", text: "Answer: the file list follows.\n\x3cacp tokens=\"2\" type=\"text\"\x3em00123\x3c/acp\x3e\nend of answer." },
    { name: "bare pair #1881", text: "Answer.\n\x3cacp\x3em00123\x3c/acp\x3e tail." },
    { name: "typo close #673", text: "Answer.\n\x3cacp tokens=\"2\" type=\"text\"\x3em00123\x3c/acip\x3e tail." },
    { name: "case drift #1731", text: "Answer.\n\x3cACP tokens=\"2\" type=\"text\"\x3em00123\x3c/ACP\x3e tail." },
    { name: "self close #2348", text: "Answer.\n\x3cacp tokens=\"2\" type=\"text\"/\x3e tail." },
    { name: "bare self close #2348", text: "Answer.\n\x3cacp/\x3e tail." },
    { name: "mangled open", text: "Answer.\n\x3cacp=1\x3e tail." },
    { name: "degen close #2190", text: "Answer.\n\x3cacp tokens=\"2\"\x3em1234\x3c/div\x3e tail." },
    { name: "refs run pair #2023", text: "Answer.\n\x3cacp tokens=\"2\" type=\"text\"\x3em00123 – m00130, m00131\x3c/acp\x3e tail." },
    { name: "prose wearing pair #1720", text: "see \x3cacp tokens=\"2\" type=\"text\"\x3ethe module notes\x3c/acp\x3e for details." },
    { name: "bare ref citation", text: "as recorded in m00123 the build stayed green." },
    { name: "orphan ref close #2023", text: "Answer. m00123 m00124\x3c/acp\x3e tail." },
    { name: "orphan ref run word-glued head #4179", text: "textm00123\u2013m00124\x3c/acp\x3e tail." },
    { name: "wrapped odd quotes", text: "Answer.\n\x3cacp tokens=\"1\" text=\"text\x3cparameter=ref\x3em00608\x3c/parameter\x3e\x3c/function\x3e tail." },
    { name: "broken attrs run-in", text: "Answer.\n\x3cacp tokens=\"1\" \x3ctool\u003e call \x3c/tool\u003e then \x3c/acp\x3e tail prose." },
    { name: "marker line #717", text: "Answer.\n📦 [ACP] Compressed m00120–m0300 → 1 block(s), ~12K tokens saved.\ntail." },
    { name: "marker line CJK", text: "见[ACP]标记的含义 stays prose.\ntail." },
    { name: "refs-only swallow past cap #644", text: "Answer.\n\x3cacp tokens=\"9\" type=\"text\"\x3em00001 m00002 m00003 m00004 m00005 m00006 m00007 m00008 m00009 m00010 m00011 m00012 m00013 m00014 m00015 m00016 m00017 m00018 m00019 m00020 m00021 m00022\x3c/acp\x3e tail." },
    { name: "mixed payload past cap", text: "Answer.\n\x3cacp tokens=\"9\" type=\"text\"\x3e" + "the quick brown fox jumps over the lazy dog ".repeat(6) + "\x3c/acp\x3e tail." },
    { name: "trunc open at EOF", text: "Answer.\n\x3cacp tokens=\"2\" type=\"text\"" },
    { name: "trunc close at EOF", text: "Answer.\n\x3c/ac" },
    { name: "trunc ref close #2348v4", text: "Answer.\nm00123\x3c/ap" },
    { name: "dead ref close #2862", text: "Answer.\n\x3cacp tokens=\"2\" type=\"text\"\x3em00123\x3c/acp\x3e then m00124\x3c/warn prose continues here." },
    { name: "two pairs adjacent", text: "A \x3cacp tokens=\"2\" type=\"text\"\x3em00123\x3c/acp\x3e B \x3cacp tokens=\"3\" type=\"text\"\x3em00124\x3c/acp\x3e C." },
    { name: "two bare opens adjacent #4163", text: "Answer.\n\x3cacp\x3e\x3cacp\x3etext tail." },
    { name: "bare open prose bare open #4163", text: "Answer.\n\x3cacp\x3emid prose\x3cacp\x3etext tail." },
    { name: "three bare opens #4163", text: "Answer.\n\x3cacp\x3e\x3cacp\x3e\x3cacp\x3etext tail." },
    { name: "bare open wrapping bare pair #4163", text: "Answer.\n\x3cacp\x3e\x3cacp\x3em00123\x3c/acp\x3etail." },
    { name: "nested bare open in attrs payload #4169", text: "A \x3cacp tokens=\"1\" type=\"text\"\x3ejunk\x3cacp\x3etext\x3c/acp\x3eB." },
    { name: "nested bare pair in attrs payload #4169", text: "A \x3cacp tokens=\"1\" type=\"text\"\x3ex\x3cacp\x3ey\x3c/acp\x3ez\x3c/acp\x3eB." },
    { name: "nested attrs pair in attrs payload #4169", text: "A \x3cacp tokens=\"1\" type=\"text\"\x3ex\x3cacp tokens=\"2\"\x3ey\x3c/acp\x3ez\x3c/acp\x3eB." },
    { name: "trunc nested open at body end #4169", text: "A \x3cacp tokens=\"1\" type=\"text\"\x3ex\x3cacp tokens=\"2 y\x3c/acp\x3ez" },
    { name: "pair split shape mid-tag", text: "Answer. \x3cacp to\nkens=\"2\" type=\"text\"\x3em00123\x3c/acp\x3e tail." },
    { name: "plain prose control", text: "the acpi.h include and caption and app stay untouched, a<b c>d." },
    { name: "empty", text: "" },
    // #4164: dead ref-close unit settled ahead of an ambiguous tail — the
    // tail's fate (acplike tag vs prose vs EOF) decides whether the unit dies
    // or stays content, and the decision must not depend on the chunk split.
    { name: "dead unit before open #4164", text: " m1234\x3c/ap\x3cACP\x3e" },
    { name: "dead unit drift name #4164", text: " m1234\x3c/ap\x3caccessp\x3e" },
    { name: "two dead units before open #4164", text: "m1111\x3c/ap x m2222\x3c/ap\x3cacp\x3e" },
    { name: "dead unit prose follow #4164", text: " m1234\x3c/ap\x3cdiv\x3emore" },
    { name: "dead unit dangling lt #4164", text: " m1234\x3c/ap\x3c" },
    { name: "dead unit non-acplike close #4164", text: " m1234\x3c/ap\x3c/div\x3e" },
];

test("tag-echo filter: chunk-boundary determinism (same input, any chunking, byte-identical output)", () => {
    const fixed = [Infinity, 1, 2, 3, 5, 8, 13, 21, 34];
    let checked = 0;
    for (const item of CORPUS) {
        const reference = runStream(item.text, [item.text.length]);
        for (const size of fixed) {
            const splits: number[] = [];
            if (size === Infinity) splits.push(item.text.length);
            else for (let left = item.text.length; left > 0; left -= size) splits.push(Math.min(size, left));
            const got = runStream(item.text, splits);
            assert.equal(got, reference, `[${item.name}] fixed chunk size ${size}: output differs from whole-buffer run`);
            checked++;
        }
        for (let seed = 0; seed < 40; seed++) {
            const got = runStream(item.text, randomSplits(item.text, seed));
            assert.equal(got, reference, `[${item.name}] random split seed ${seed}: output differs from whole-buffer run`);
            checked++;
        }
    }
    console.log(`chunking determinism: ${CORPUS.length} corpus items x ${fixed.length + 40} chunkings = ${checked} runs, all byte-identical`);
});

test("tag-echo filter: streaming result vs whole-text stripAcpTags (informational parity)", () => {
    let same = 0;
    const diffs: string[] = [];
    for (const item of CORPUS) {
        const streamed = runStream(item.text, [item.text.length]);
        const whole = stripAcpTags(item.text);
        if (streamed === whole) same++;
        else diffs.push(`${item.name}: streamed=${JSON.stringify(streamed)} whole=${JSON.stringify(whole)}`);
    }
    console.log(`streaming vs whole-text parity: ${same}/${CORPUS.length} identical`);
    for (const d of diffs) console.log(`  differs — ${d}`);
});

test("marker-line filter: chunk-boundary determinism", () => {
    const texts = [
        "Answer.\n📦 [ACP] Compressed m00120–m0300 → 1 block(s), ~12K tokens saved.\ntail.",
        "见[ACP]标记的含义 stays prose.\ntail.",
        "no markers here at all.",
    ];
    for (const text of texts) {
        const run = (splits: number[]): string => {
            const f = createMarkerLineFilter();
            let out = "";
            let pos = 0;
            for (const len of splits) {
                out += f.push(text.slice(pos, pos + len));
                pos += len;
            }
            if (pos < text.length) out += f.push(text.slice(pos));
            return out + f.flush();
        };
        const reference = run([text.length]);
        for (const size of [1, 2, 3, 5, 8]) {
            const splits: number[] = [];
            for (let left = text.length; left > 0; left -= size) splits.push(Math.min(size, left));
            assert.equal(run(splits), reference, `marker filter chunk size ${size} differs`);
        }
        for (let seed = 0; seed < 20; seed++) assert.equal(run(randomSplits(text, seed)), reference, `marker filter seed ${seed} differs`);
    }
});

test("bili-artifact filter: chunk-boundary determinism (chain carrier)", () => {
    const carrier = "\x3cbili-chain v=\"1\" n=\"7\" fp=\"ab12cd34\"/\x3e";
    const text = `Answer.\n${carrier}\ntail.`;
    const run = (splits: number[]): string => {
        const f = createBiliArtifactFilter();
        let out = "";
        let pos = 0;
        for (const len of splits) {
            out += f.push(text.slice(pos, pos + len));
            pos += len;
        }
        if (pos < text.length) out += f.push(text.slice(pos));
        return out + f.flush();
    };
    const reference = run([text.length]);
    for (const size of [1, 2, 3, 5, 8, 13]) {
        const splits: number[] = [];
        for (let left = text.length; left > 0; left -= size) splits.push(Math.min(size, left));
        assert.equal(run(splits), reference, `artifact filter chunk size ${size} differs`);
    }
    for (let seed = 0; seed < 20; seed++) assert.equal(run(randomSplits(text, seed)), reference, `artifact filter seed ${seed} differs`);
});

test("dsml-leak filter: strips malformed fullwidth DSML tags with 100% chunk determinism and whole-text parity", () => {
    const cases: Array<{ name: string; input: string; expected: string }> = [
        {
            name: "trailing orphan close stack",
            input: "做成可编程接口。\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C parameter\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C calls\x3e",
            expected: "做成可编程接口。",
        },
        {
            name: "orphan DSML-only turn empties cleanly",
            input: "\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C calls\x3e",
            expected: "\n",
        },
        {
            name: "malformed open calls and nameless invoke",
            input: "检查一下：\x3c\uFF5CDSML\uFF5C calls\x3e \x3c\uFF5CDSML\uFF5C invoke \x3e\n继续正文。",
            expected: "检查一下：继续正文。",
        },
        {
            name: "standalone space-separated DSML pair empties cleanly",
            input: "\x3c\uFF5CDSML\uFF5C calls\x3e \x3c\uFF5CDSML\uFF5C invoke \x3e",
            expected: "",
        },
        {
            name: "DSML tag followed by space and non-DSML markup keeps the space",
            input: "\x3c\uFF5CDSML\uFF5C calls\x3e \x3chello\x3e",
            expected: " \x3chello\x3e",
        },
        {
            name: "DSML tag followed by space and unterminated parameter prefix releases both at EOF",
            input: "\x3c\uFF5CDSML\uFF5C calls\x3e \x3cparameter",
            expected: " \x3cparameter",
        },
        {
            name: "DSML tag followed by trailing spaces at EOF drops spaces cleanly",
            input: "正文\x3c\uFF5CDSML\uFF5C calls\x3e   ",
            expected: "正文",
        },
        {
            name: "DSML tag followed by space and lone CR preserves both",
            input: "\x3c\uFF5CDSML\uFF5C calls\x3e \r",
            expected: " \r",
        },
        {
            name: "bc ref fused into parameter tag + DSML close",
            input: "\n\x3cparameter name=\"m98078\"\x3em98078\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C parameter\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C calls\x3e",
            expected: "\n",
        },
        {
            name: "backtick-quoted DSML mention in prose is preserved",
            input: "结果输出了半截乱码（`\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e`）。",
            expected: "结果输出了半截乱码（`\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e`）。",
        },
        {
            name: "normal HTML and ASCII pipes are untouched",
            input: "\x3cdiv\x3ea | b\x3c/div\x3e\n\x3c|not-fullwidth|\x3e",
            expected: "\x3cdiv\x3ea | b\x3c/div\x3e\n\x3c|not-fullwidth|\x3e",
        },
    ];

    for (const c of cases) {
        assert.equal(stripDsmlLeaks(c.input), c.expected, `whole-text mismatch for ${c.name}`);
        const run = (splits: number[]): string => {
            const f = createDsmlLeakFilter();
            let out = "";
            let pos = 0;
            for (const len of splits) {
                out += f.push(c.input.slice(pos, pos + len));
                pos += len;
            }
            if (pos < c.input.length) out += f.push(c.input.slice(pos));
            return out + f.flush();
        };
        assert.equal(run([c.input.length]), c.expected, `whole-buffer stream mismatch for ${c.name}`);
        for (const size of [1, 2, 3, 5, 8, 13]) {
            const splits: number[] = [];
            for (let left = c.input.length; left > 0; left -= size) splits.push(Math.min(size, left));
            assert.equal(run(splits), c.expected, `chunk size ${size} mismatch for ${c.name}`);
        }
        for (let seed = 0; seed < 20; seed++) {
            assert.equal(run(randomSplits(c.input, seed)), c.expected, `random seed ${seed} mismatch for ${c.name}`);
        }
    }
});

test("leading-newline trimmer composes after tag-echo + DSML filters with 100% chunk determinism and whole-text parity", () => {
    const cases: Array<{ input: string; expected: string }> = [
        { input: "\n\n\n\n\n先给结论：\n\n第二段", expected: "先给结论：\n\n第二段" },
        { input: "\r\n\r\n  indented code\n  line 2", expected: "  indented code\n  line 2" },
        { input: "\x3cacp tokens=\"240\" type=\"text\"\x3em01234\x3c/acp\x3e\n\n\n正文开始", expected: "正文开始" },
        { input: "\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C invoke\x3e\n\x3c/\uFF5C\uFF5CDSML\uFF5C\uFF5C calls\x3e", expected: "" },
    ];
    for (const c of cases) {
        const whole = trimLeadingNewlines(stripDsmlLeaks(stripAcpTags(c.input)));
        assert.equal(whole, c.expected);
        const respEv = stripResponsesText({ type: "response.output_text.done", text: c.input }, false, undefined, { stripDsml: true, trimLeadingNewlines: true });
        assert.equal(respEv.text, c.expected);

        const run = (size: number): string => {
            const f = composeStreamFilters(composeStreamFilters(createTagEchoFilter(), createDsmlLeakFilter()), createLeadingNewlineFilter());
            let out = "";
            for (let i = 0; i < c.input.length; i += size) out += f.push(c.input.slice(i, i + size));
            return out + f.flush();
        };
        for (const size of [1, 2, 3, 7, c.input.length]) {
            assert.equal(run(size), c.expected, `composed chunk size ${size} mismatch`);
        }
    }
});
