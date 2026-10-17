import { compareSeq, mergeActivities } from "./activity-merge.ts";
import assert from "node:assert/strict";
import test from "node:test";

const activity = (seq, kind = "tool_use", extra = {}) => ({
  seq: String(seq),
  kind,
  worker: "auto",
  ts: "2026-09-10T07:00:00Z",
  summary: String(seq),
  ...extra,
});

test("overlapping poll responses render tool 198 only once", () => {
  const first = [activity("197"), activity("198")];
  const second = [activity("198"), activity("199", "tool_result")];
  const merged = mergeActivities(mergeActivities([], first), second);
  assert.deepEqual(
    merged.map((item) => item.seq),
    ["197", "198", "199"],
  );
  assert.deepEqual(mergeActivities(merged, second), merged);
});

test("a late response and overlapping older page preserve newer messages", () => {
  let rows = mergeActivities([activity("198")], [activity("199"), activity("200")]);
  rows = mergeActivities(rows, [activity("198"), activity("199")]);
  rows = mergeActivities([activity("196"), activity("197"), activity("198")], rows);
  assert.deepEqual(
    rows.map((item) => item.seq),
    ["196", "197", "198", "199", "200"],
  );
});

test("replayed results and duplicate entries do not double-count usage", () => {
  const result = activity("201", "result", { input_tokens: 120, output_tokens: 30 });
  const rows = mergeActivities([result, result], [result, result]);
  assert.equal(rows.length, 1);
  assert.equal(
    rows.reduce((sum, item) => sum + item.input_tokens, 0),
    120,
  );
});

test("merging does not mutate either response and keeps distinct IDs", () => {
  const first = Object.freeze([Object.freeze(activity("199"))]);
  const second = Object.freeze([Object.freeze(activity("198")), Object.freeze(activity("200"))]);
  assert.deepEqual(
    mergeActivities(first, second).map((item) => item.seq),
    ["198", "199", "200"],
  );
  assert.equal(first.length, 1);
  assert.equal(second.length, 2);
});

test("avoids dictionary order trap (e.g. ['1000','999','1001'] sorts numerically to ['999','1000','1001'])", () => {
  const incoming = [activity("1000"), activity("999"), activity("1001")];
  const merged = mergeActivities([], incoming);
  assert.deepEqual(
    merged.map((item) => item.seq),
    ["999", "1000", "1001"],
  );
  assert.equal(compareSeq("999", "1000"), -1);
  assert.equal(compareSeq("1000", "999"), 1);
  assert.equal(compareSeq("1000", "1000"), 0);
});

test("large 64-bit ID precision (>2^53) is preserved and distinct keys are not merged", () => {
  // 9007199254740992n (2^53) vs 9007199254740993n (2^53 + 1)
  const a = activity("9007199254740992", "tool_use", { input_tokens: 10 });
  const b = activity("9007199254740993", "tool_result", { input_tokens: 20 });
  const maxInt64 = activity("9223372036854775807", "result", { input_tokens: 30 });

  const merged = mergeActivities([], [maxInt64, b, a]);
  assert.deepEqual(
    merged.map((item) => item.seq),
    ["9007199254740992", "9007199254740993", "9223372036854775807"],
  );
  assert.equal(merged.length, 3);
  assert.equal(
    merged.reduce((sum, item) => sum + item.input_tokens, 0),
    60,
  );

  // Exact duplicates still deduplicate by string seq
  const deduped = mergeActivities(merged, [activity("9007199254740993", "tool_result", { input_tokens: 20 })]);
  assert.equal(deduped.length, 3);
  assert.equal(
    deduped.reduce((sum, item) => sum + item.input_tokens, 0),
    60,
  );
});
