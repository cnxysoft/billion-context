import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { openSqliteDatabase } from "../src/storage/driver.ts";
import { describeCoreMessage, describeMessageContent, identityOfStoredContent, serializeAndDescribeCoreMessage, splitMediaContent } from "../src/storage/identity.ts";
import { migrateSchema } from "../src/storage/schema.ts";
import { rmrf } from "./tmp-rm.ts";

function sha256(raw: string): string {
    return createHash("sha256").update(raw).digest("hex");
}

test("non-object stored content preserves its raw-byte identity", () => {
    for (const raw of ["null", " null ", "true", "false", "42", '"text"', "[]", '[{"id":"inside-array"}]', "not JSON"]) {
        assert.equal(identityOfStoredContent(raw), sha256(raw), raw);
    }
});

test("message identities still exclude positional metadata and retain content", () => {
    const first = JSON.stringify({ id: "old", wireIndex: 0, ledgerSeq: 1, role: "user", content: "hello" });
    const second = JSON.stringify({ id: "new", wireIndex: 7, ledgerSeq: 8, role: "user", content: "hello" });
    assert.equal(identityOfStoredContent(first), identityOfStoredContent(second));
    assert.notEqual(identityOfStoredContent(first), identityOfStoredContent(second.replace("hello", "changed")));
});

test("planned message content agrees with the stored identity on plain and media rows", () => {
    const message = {
        id: "row", wireIndex: 3, ledgerSeq: 9, role: "assistant", contentType: "text",
        rawOpenaiContentParts: [{ type: "thinking", thinking: "原文", signature: "signed\r\nbytes" }],
    };
    const base64 = "YWJj".repeat(600);
    for (const row of [
        message,
        { ...message, imageBase64: base64 },
        { ...message, rawOpenaiContentParts: [{ type: "image_url", image_url: { url: `data:image/png;base64,${base64}` } }] },
    ]) {
        const raw = JSON.stringify(row);
        const split = splitMediaContent(raw);
        assert.deepEqual(describeMessageContent(raw), {
            ...split, role: row.role, contentType: row.contentType,
            identity: identityOfStoredContent(split.content),
        });
        assert.deepEqual(describeCoreMessage(row, raw), describeMessageContent(raw));
        assert.deepEqual(serializeAndDescribeCoreMessage(row), describeMessageContent(raw));
    }
    const noPosition = { role: "user", content: "plain" };
    assert.deepEqual(describeCoreMessage(noPosition, JSON.stringify(noPosition)), describeMessageContent(JSON.stringify(noPosition)));
    assert.deepEqual(serializeAndDescribeCoreMessage(noPosition), describeMessageContent(JSON.stringify(noPosition)));
    const withTs = { ...message, ts: 1700000000000 };
    const withoutTsRaw = JSON.stringify(message);
    assert.deepEqual(describeCoreMessage(withTs, withoutTsRaw, { stripTs: true }), describeMessageContent(withoutTsRaw));
    assert.deepEqual(serializeAndDescribeCoreMessage(withTs, { stripTs: true }), describeMessageContent(withoutTsRaw));
    const withTsRaw = JSON.stringify(withTs);
    assert.deepEqual(describeCoreMessage(withTs, withTsRaw), describeMessageContent(withTsRaw));
    assert.deepEqual(serializeAndDescribeCoreMessage(withTs), describeMessageContent(withTsRaw));
    const customToJson = { ...message, toJSON: () => ({ role: "user", content: "from-toJSON", id: "x" }) };
    const customRaw = JSON.stringify(customToJson);
    assert.deepEqual(describeCoreMessage(customToJson, customRaw), describeMessageContent(customRaw));
    assert.deepEqual(serializeAndDescribeCoreMessage(customToJson), describeMessageContent(customRaw));

    // Edge shapes for single-stringify splice: leading id + trailing wireIndex/ledgerSeq,
    // mid-object positional key (fallback), undefined fields, and integer keys.
    for (const shape of [
        { id: "m1", role: "user", text: "hi", wireIndex: 4, ledgerSeq: 5, ts: 99 },
        { role: "user", id: "mid", text: "between", wireIndex: 1 },
        { omitted: undefined, id: "m2", role: "assistant", text: "ok", ledgerSeq: 2 },
        { id: "m3", "0": "idx", role: "user", text: "int-key", wireIndex: 3 },
        { id: "m4", role: "user", wireIndex: undefined, ledgerSeq: 9 },
    ]) {
        const copy: Record<string, unknown> = { ...shape };
        delete copy.ts;
        const expectedRaw = JSON.stringify(copy);
        assert.deepEqual(serializeAndDescribeCoreMessage(shape, { stripTs: true }), describeMessageContent(expectedRaw));
    }

    // Verify fast path stringifies the large message body only ONCE.
    let objectStringifies = 0;
    const typical = { id: "m-fast", role: "user", text: "payload".repeat(200), wireIndex: 7, ledgerSeq: 8, ts: 123 };
    const expectedTypical = describeMessageContent(JSON.stringify({ id: "m-fast", role: "user", text: typical.text, wireIndex: 7, ledgerSeq: 8 }));
    const fastRes = serializeAndDescribeCoreMessage(typical, {
        stripTs: true,
        stringify: (v) => {
            if (v !== null && typeof v === "object") objectStringifies++;
            return JSON.stringify(v);
        },
    });
    assert.deepEqual(fastRes, expectedTypical);
    assert.equal(objectStringifies, 1, "leading/trailing positional keys stringify the message object only once");
});

test("v8 identity migration can finish with a JSON-null content row", () => {
    const dir = fs.mkdtempSync(path.join(tmpdir(), "bili-null-identity-"));
    const previousZstd = process.env.BILI_PERSIST_ZSTD;
    const previousConfig = process.env.BILI_CONFIG_FILE;
    process.env.BILI_PERSIST_ZSTD = "0";
    process.env.BILI_CONFIG_FILE = path.join(dir, "no-config.json");
    let db: ReturnType<typeof openSqliteDatabase>["db"] | undefined;
    try {
        db = openSqliteDatabase(path.join(dir, "sessions.db")).db;
        migrateSchema(db);
        const raw = "null";
        const hash = sha256(raw);
        db.prepare("INSERT INTO content (hash, byte_len, refcount, algo, bytes) VALUES (?, ?, 1, 'sha256', ?)").run(hash, Buffer.byteLength(raw), Buffer.from(raw));
        db.prepare("INSERT INTO messages (session_id, seq, msg_id, role, kind, content_hash) VALUES (?, ?, ?, ?, 'message', ?)").run("null-session", 1, "null-message", "user", hash);
        db.exec("UPDATE messages SET identity = ''");
        db.exec("PRAGMA user_version = 7");

        const migrated = migrateSchema(db);
        assert.equal(migrated.from, 7);
        assert.equal(migrated.to, 12);
        const row = db.prepare("SELECT identity, content_hash FROM messages WHERE session_id = ?").get("null-session") as { identity: string; content_hash: string };
        assert.equal(row.identity, hash);
        assert.equal(row.content_hash, hash);
        const content = db.prepare("SELECT bytes, refcount FROM content WHERE hash = ?").get(hash) as { bytes: Uint8Array; refcount: number };
        assert.equal(Buffer.from(content.bytes).toString(), raw);
        assert.equal(content.refcount, 1);
        assert.equal(migrateSchema(db).from, 12);
    } finally {
        db?.close();
        if (previousZstd === undefined) delete process.env.BILI_PERSIST_ZSTD;
        else process.env.BILI_PERSIST_ZSTD = previousZstd;
        if (previousConfig === undefined) delete process.env.BILI_CONFIG_FILE;
        else process.env.BILI_CONFIG_FILE = previousConfig;
        rmrf(dir);
    }
});
