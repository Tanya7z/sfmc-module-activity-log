import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { takeBatch } from "../sapi/src/queue-policy.js";

describe("activity-log queue policy", () => {
  it("单次只取 batch_size 条并保留剩余队列", () => {
    const queue = Array.from({ length: 250 }, (_, i) => i);

    assert.deepEqual(takeBatch(queue, 100), Array.from({ length: 100 }, (_, i) => i));
    assert.equal(queue.length, 150);
    assert.equal(queue[0], 100);
  });

  it("队列不足一个批次时取出全部记录", () => {
    const queue = [1, 2, 3];

    assert.deepEqual(takeBatch(queue, 100), [1, 2, 3]);
    assert.deepEqual(queue, []);
  });
});
