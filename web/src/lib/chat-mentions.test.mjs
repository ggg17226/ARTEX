import assert from "node:assert/strict";
import test from "node:test";
import { activeMention, mentionSearch, mentionToken, selectedMentions } from "./chat-mentions.ts";

test("mention trigger supports Chinese and cursor placement without hijacking email", () => {
  assert.equal(activeMention("user@example.com", 16), null);
  assert.equal(activeMention("已选 @[漏洞#1 X]", 12), null);
  assert.deepEqual(activeMention("查看@漏洞 后面的文字", 5), { start: 2, end: 5, query: "漏洞" });
  assert.equal(activeMention("@漏洞\n下一行", 8), null);
});

test("categories, Chinese aliases, IP and keyword search", () => {
  assert.equal(mentionSearch("").categories.length, 9);
  assert.equal(mentionSearch("漏").categories[0].kind, "finding");
  assert.equal(mentionSearch("漏洞").kind, "finding");
  assert.equal(mentionSearch("漏洞SQL注入").query, "SQL注入");
  assert.equal(mentionSearch("ip 192.0.2.1").kind, "ip");
  assert.equal(mentionSearch("接口 GET /api").query, "GET /api");
  assert.equal(mentionSearch("acme.com").kind, "");
});

test("tokens roundtrip labels and removing one reference preserves its neighbors", () => {
  const first = mentionToken({ kind: "finding", id: "12", label: "标题[1]\n描述" });
  const second = mentionToken({ kind: "ip", id: "13", label: "192.0.2.1" });
  const value = `分析 ${first} 和 ${second}`;
  const selected = selectedMentions(value);
  assert.equal(selected.length, 2);
  assert.equal(selected[0].label, "漏洞 #12 · 标题（1） 描述");
  const next = value.slice(0, selected[0].start) + value.slice(selected[0].start + selected[0].token.length);
  assert.equal(selectedMentions(next)[0].token, second);
});

test("large 64-bit ID (>2^53 and MaxInt64) tokens roundtrip without loss of precision", () => {
  const maxInt64 = "9223372036854775807";
  const beyondSafe = "9007199254740993";
  const tokenMax = mentionToken({ kind: "finding", id: maxInt64, label: "严重漏洞" });
  const tokenBeyond = mentionToken({ kind: "asset", id: beyondSafe, label: "关键资产" });

  assert.equal(tokenMax, `@[漏洞#${maxInt64} 严重漏洞]`);
  assert.equal(tokenBeyond, `@[资产#${beyondSafe} 关键资产]`);

  const selected = selectedMentions(`测试 ${tokenMax} 与 ${tokenBeyond}`);
  assert.equal(selected.length, 2);
  assert.equal(selected[0].token, tokenMax);
  assert.equal(selected[0].label, `漏洞 #${maxInt64} · 严重漏洞`);
  assert.equal(selected[1].token, tokenBeyond);
  assert.equal(selected[1].label, `资产 #${beyondSafe} · 关键资产`);
});
