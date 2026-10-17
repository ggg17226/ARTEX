import type { Activity } from "./types";

// P03：Activity.seq 是 activity 主键（wire string）。数值比较必须走 BigInt ——
// 字典序会把 "1000" 判成比 "999" 小，Number() 又会在 >2^53 时丢精度。
// 返回升序比较结果；需要倒序时交换两个入参。
export function compareSeq(left: string, right: string): number {
  const diff = BigInt(left) - BigInt(right);
  if (diff < BigInt(0)) return -1;
  if (diff > BigInt(0)) return 1;
  return 0;
}

// History and live responses can overlap or arrive out of order. A persisted
// activity's seq is its identity; replay must not duplicate rows or token totals.
export function mergeActivities(current: Activity[], incoming: Activity[]): Activity[] {
  const bySeq = new Map<string, Activity>();
  for (const activity of current) bySeq.set(activity.seq, activity);
  for (const activity of incoming) bySeq.set(activity.seq, activity);
  return [...bySeq.values()].sort((left, right) => compareSeq(left.seq, right.seq));
}
