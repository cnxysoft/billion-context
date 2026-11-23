import type { CoreMessage } from "./types.js";

function isAssistantAct(msg: CoreMessage): boolean {
  return (
    msg.role === "assistant" &&
    (msg.contentType === "text" || msg.contentType === "tool-call")
  );
}

/**
 * Atomic turn groups for compression integrity (#684).
 *
 * A turn is a reasoning run, the assistant text/tool-call burst that follows
 * it, and every tool-result paired (by toolCallId) to the burst's calls.
 * Strict-echo thinking providers (DeepSeek: "The `reasoning_content` in the
 * thinking mode must be passed back to the API") reject a rebuilt request
 * whose assistant tool-call turn survives without its reasoning, and every
 * OpenAI-wire provider rejects a result whose call is gone. Turns are
 * therefore atomic for folding: all members fold together, or none do.
 *
 * Grouping is adjacency-based, mirroring {@link adjustBoundariesForReasoningPairs}:
 * a reasoning run pairs with the assistant burst immediately following it.
 * Bursts without a preceding reasoning run still group with their sibling
 * calls and results. Messages belonging to no turn (user messages, system,
 * orphan reasoning, summaries) get no group — they carry no pairing
 * constraint. Groups are disjoint.
 *
 * @returns disjoint member-id arrays; a message appears in at most one group.
 */
export function computeTurnGroups(
  messages: CoreMessage[],
  prebuiltResultIdByCallId?: ReadonlyMap<string, string>,
): string[][] {
  let resultIdByCallId: ReadonlyMap<string, string>;
  if (prebuiltResultIdByCallId !== undefined) {
    resultIdByCallId = prebuiltResultIdByCallId;
  } else {
    const builtMap = new Map<string, string>();
    for (const msg of messages) {
      if (
        msg.contentType === "tool-result" &&
        typeof msg.toolCallId === "string" &&
        msg.id
      ) {
        if (!builtMap.has(msg.toolCallId))
          builtMap.set(msg.toolCallId, msg.id);
      }
    }
    resultIdByCallId = builtMap;
  }

  const grouped = new Set<string>();
  const groups: string[][] = [];
  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i]!;
    if (!msg.id || grouped.has(msg.id)) continue;
    if (!(msg.contentType === "reasoning" || isAssistantAct(msg))) continue;

    let reasoningStart = i;
    if (msg.contentType === "reasoning") {
      while (
        reasoningStart > 0 &&
        messages[reasoningStart - 1]!.contentType === "reasoning"
      ) {
        reasoningStart--;
      }
    } else {
      let s = i;
      while (s > 0 && isAssistantAct(messages[s - 1]!)) s--;
      reasoningStart = s;
      while (
        reasoningStart > 0 &&
        messages[reasoningStart - 1]!.contentType === "reasoning"
      ) {
        reasoningStart--;
      }
    }
    const burstStart = (() => {
      let s = reasoningStart;
      while (s < messages.length && messages[s]!.contentType === "reasoning")
        s++;
      return s;
    })();
    if (
      burstStart >= messages.length ||
      !isAssistantAct(messages[burstStart]!)
    ) {
      // Orphan reasoning run (no companion burst): no pairing constraint.
      continue;
    }
    let burstEnd = burstStart;
    while (
      burstEnd + 1 < messages.length &&
      isAssistantAct(messages[burstEnd + 1]!)
    ) {
      burstEnd++;
    }

    const members = new Set<string>();
    for (let k = reasoningStart; k <= burstEnd; k++) {
      const m = messages[k]!;
      if (!m.id) continue;
      members.add(m.id);
      if (
        m.role === "assistant" &&
        m.contentType === "tool-call" &&
        typeof m.toolCallId === "string"
      ) {
        const rid = resultIdByCallId.get(m.toolCallId);
        if (rid) members.add(rid);
      }
    }
    for (const id of members) grouped.add(id);
    groups.push([...members]);
  }
  return groups;
}

/** Which folded ids the rebuild cannot keep folded, and how many turns and
 *  call/result pairs that covers. */
export interface IntegrityWithdrawals {
  withdrawn: Set<string>;
  splitTurnCount: number;
  splitPairCount: number;
}

/** Precomputed per-MESSAGE inputs of {@link computeIntegrityWithdrawals} (#5337
 *  option A): everything that depends only on `messages`, not on `foldedIds`,
 *  is an O(n) pass repeated for every screened unit — hoisting it to one
 *  build-per-turn cuts the dominant term of the O(n²) recommend screening.
 *  Build via {@link buildIntegrityPre}; omit the parameter to compute inline
 *  (single-call paths keep the legacy behavior). */
export interface IntegrityPre {
  reasoningIds: Set<string>;
  callIds: Set<string>;
  /** The tool-call messages themselves (id present) — the fixed-point pair
   *  loop iterates these instead of re-scanning ALL messages per pass. */
  callMessages: CoreMessage[];
  callIdByMessageId: Map<string, string>;
  resultIdByCallId: Map<string, string>;
  groups: string[][];
  /** Reasoning message id -> index in `groups` (each reasoning message belongs
   *  to at most one group). Enables O(|foldedIds|) candidate-group filtering. */
  groupIndexByReasoningId?: Map<string, number>;
  /** Message id (call or result) -> sorted indices in `callMessages` whose
   *  (call, result) pair includes that message id. Enables O(|foldedIds|)
   *  candidate-pair filtering. */
  pairIndicesByMsgId?: Map<string, number[]>;
}

/** One-pass build of {@link IntegrityPre} for a message array. */
export function buildIntegrityPre(messages: CoreMessage[]): IntegrityPre {
  const reasoningIds = new Set<string>();
  const callIds = new Set<string>();
  const callMessages: CoreMessage[] = [];
  const callIdByMessageId = new Map<string, string>();
  const resultIdByCallId = new Map<string, string>();
  for (const m of messages) {
    if (!m.id) continue;
    if (m.contentType === "reasoning") reasoningIds.add(m.id);
    if (m.role === "assistant" && m.contentType === "tool-call") {
      callIds.add(m.id);
      callMessages.push(m);
      if (typeof m.toolCallId === "string") {
        callIdByMessageId.set(m.id, m.toolCallId);
      }
    }
    if (m.contentType === "tool-result" && typeof m.toolCallId === "string") {
      if (!resultIdByCallId.has(m.toolCallId)) {
        resultIdByCallId.set(m.toolCallId, m.id);
      }
    }
  }
  const groups = computeTurnGroups(messages, resultIdByCallId);
  const groupIndexByReasoningId = new Map<string, number>();
  for (let g = 0; g < groups.length; g++) {
    const group = groups[g]!;
    for (let k = 0; k < group.length; k++) {
      const id = group[k]!;
      if (reasoningIds.has(id)) groupIndexByReasoningId.set(id, g);
    }
  }
  const pairIndicesByMsgId = new Map<string, number[]>();
  const pushPairIndex = (msgId: string, idx: number): void => {
    let list = pairIndicesByMsgId.get(msgId);
    if (list === undefined) {
      list = [idx];
      pairIndicesByMsgId.set(msgId, list);
    } else {
      list.push(idx);
    }
  };
  for (let idx = 0; idx < callMessages.length; idx++) {
    const m = callMessages[idx]!;
    const callId = callIdByMessageId.get(m.id);
    if (callId === undefined) continue;
    const resultId = resultIdByCallId.get(callId);
    if (resultId === undefined) continue;
    pushPairIndex(m.id, idx);
    if (resultId !== m.id) pushPairIndex(resultId, idx);
  }
  return {
    reasoningIds,
    callIds,
    callMessages,
    callIdByMessageId,
    resultIdByCallId,
    groups,
    groupIndexByReasoningId,
    pairIndicesByMsgId,
  };
}

/**
 * Fold-integrity gate (#684): given the ids a fold removes from the visible
 * stream, return the ids that must stay visible instead.
 *
 *   - INV1 — a kept assistant tool-call must keep its reasoning run, so a turn
 *     whose reasoning folds while a call of the same turn stays visible is
 *     withdrawn wholesale (all members stay visible).
 *   - INV2 — a tool-call and its tool-result are one exchange: a pair the fold
 *     would split is withdrawn together.
 *
 * The two invariants feed each other. Withdrawing a call for INV2 leaves a turn
 * whose reasoning still folds while its call now stays visible — exactly the
 * INV1 split — and withdrawing a turn for INV1 can strand a result whose call
 * survived outside the turn. One pass in a fixed order misses both, so the gate
 * repeats until nothing changes; a turn or pair is acted on once and reported
 * once.
 *
 * The reverse direction stays allowed (#564): reasoning and text may stay
 * visible while the call and its result fold, since that leaves a valid stream.
 *
 * Coverage is not "kept". An id in `invisibleIds` is already hidden by an
 * active block: it neither counts as folding nor as kept-visible. A covered
 * tool-call therefore cannot strand its turn's reasoning — without this, a
 * #564 reverse split (call+result inside an existing block, reasoning+text
 * visible) withdraws the stranded pair wholesale FOREVER: they leave no gap a
 * range can span, so every later fold reclaims only everything else and the
 * residue grows monotonically until nothing left is big enough to fold
 * (billion-context#5531). Omit the parameter for the legacy model where
 * everything not folded reads as visible.
 */
export function computeIntegrityWithdrawals(
  messages: CoreMessage[],
  foldedIds: ReadonlySet<string>,
  pre?: IntegrityPre,
  invisibleIds?: ReadonlySet<string>,
): IntegrityWithdrawals {
  if (foldedIds.size === 0) {
    return { withdrawn: new Set<string>(), splitTurnCount: 0, splitPairCount: 0 };
  }
  const remaining = new Set(foldedIds);
  const withdrawn = new Set<string>();
  const handledTurns = new Set<number>();
  const handledPairs = new Set<string>();

  const built = pre ?? buildIntegrityPre(messages);
  const reasoningIds = built.reasoningIds;
  const callIds = built.callIds;
  const callIdByMessageId = built.callIdByMessageId;
  const resultIdByCallId = built.resultIdByCallId;
  const groups = built.groups;
  const groupIndexByReasoningId = built.groupIndexByReasoningId;
  const pairIndicesByMsgId = built.pairIndicesByMsgId;

  // `remaining` only shrinks across fixed-point iterations, so any group that
  // can ever fire INV1 must already have a reasoning id in initial `foldedIds`,
  // and any (call, result) pair that can ever fire INV2 must already have at
  // least one endpoint in initial `foldedIds`. Sorting ascending preserves
  // exact scan order over `groups` and `callMessages`.
  let candidateGroups: number[] | null = null;
  if (groupIndexByReasoningId !== undefined) {
    const seen = new Set<number>();
    for (const id of remaining) {
      const g = groupIndexByReasoningId.get(id);
      if (g !== undefined) seen.add(g);
    }
    candidateGroups = [...seen].sort((a, b) => a - b);
  }
  let candidatePairIndices: number[] | null = null;
  if (pairIndicesByMsgId !== undefined) {
    const seen = new Set<number>();
    for (const id of remaining) {
      const indices = pairIndicesByMsgId.get(id);
      if (indices !== undefined) {
        for (let k = 0; k < indices.length; k++) seen.add(indices[k]!);
      }
    }
    candidatePairIndices = [...seen].sort((a, b) => a - b);
  }

  let changed = true;
  while (changed) {
    changed = false;

    if (candidateGroups !== null) {
      for (let ci = 0; ci < candidateGroups.length; ci++) {
        const g = candidateGroups[ci]!;
        if (handledTurns.has(g)) continue;
        const group = groups[g]!;
        const foldHasReasoning = group.some(
          (id) => remaining.has(id) && reasoningIds.has(id),
        );
        if (!foldHasReasoning) continue;
        const keptHasCall = group.some(
          (id) =>
            !remaining.has(id) &&
            callIds.has(id) &&
            !invisibleIds?.has(id),
        );
        if (!keptHasCall) continue;
        handledTurns.add(g);
        for (const id of group) {
          remaining.delete(id);
          withdrawn.add(id);
        }
        changed = true;
      }
    } else {
      for (let g = 0; g < groups.length; g++) {
        if (handledTurns.has(g)) continue;
        const group = groups[g]!;
        const foldHasReasoning = group.some(
          (id) => remaining.has(id) && reasoningIds.has(id),
        );
        if (!foldHasReasoning) continue;
        const keptHasCall = group.some(
          (id) =>
            !remaining.has(id) &&
            callIds.has(id) &&
            !invisibleIds?.has(id),
        );
        if (!keptHasCall) continue;
        handledTurns.add(g);
        for (const id of group) {
          remaining.delete(id);
          withdrawn.add(id);
        }
        changed = true;
      }
    }

    if (candidatePairIndices !== null) {
      for (let ci = 0; ci < candidatePairIndices.length; ci++) {
        const m = built.callMessages[candidatePairIndices[ci]!]!;
        const callId = callIdByMessageId.get(m.id);
        if (callId === undefined || handledPairs.has(callId)) continue;
        const resultId = resultIdByCallId.get(callId);
        if (resultId === undefined) continue;
        if (remaining.has(m.id) === remaining.has(resultId)) continue;
        handledPairs.add(callId);
        remaining.delete(m.id);
        remaining.delete(resultId);
        withdrawn.add(m.id);
        withdrawn.add(resultId);
        changed = true;
      }
    } else {
      for (const m of built.callMessages) {
        const callId = callIdByMessageId.get(m.id);
        if (callId === undefined || handledPairs.has(callId)) continue;
        const resultId = resultIdByCallId.get(callId);
        if (resultId === undefined) continue;
        if (remaining.has(m.id) === remaining.has(resultId)) continue;
        handledPairs.add(callId);
        remaining.delete(m.id);
        remaining.delete(resultId);
        withdrawn.add(m.id);
        withdrawn.add(resultId);
        changed = true;
      }
    }
  }

  return {
    withdrawn,
    splitTurnCount: handledTurns.size,
    splitPairCount: handledPairs.size,
  };
}
