/**
 * queue-policy.ts — 队列消费与容量控制策略
 */

export const DEFAULT_MAX_QUEUE_SIZE = 5000;

/**
 * 从待刷盘队列中取出一个有界批次，避免单次事务长期独占数据库连接。
 *
 * @param queue 目标队列数组
 * @param batchSize 单批次最大容量
 * @returns 切割出的批次数组
 */
export function takeBatch<T>(queue: T[], batchSize: number): T[] {
  const size = Math.max(1, Math.floor(batchSize));
  return queue.splice(0, size);
}

/**
 * 安全入队：控制内存队列上限，防止数据库中断或突发事件导致内存泄漏 (OOM)。
 * 当队列超出上限时，淘汰最旧的记录以腾出空间。
 *
 * @param queue 目标队列数组
 * @param item 待入队项
 * @param maxQueueSize 允许的最大队列深度
 * @returns 被淘汰丢弃的条目数量
 */
export function safeEnqueue<T>(queue: T[], item: T, maxQueueSize: number = DEFAULT_MAX_QUEUE_SIZE): number {
  let dropped = 0;
  if (queue.length >= maxQueueSize) {
    // 超过上限时，批量丢弃最旧的 10% 记录，避免频繁单个 splice 开销
    const dropCount = Math.max(1, Math.floor(maxQueueSize * 0.1));
    queue.splice(0, dropCount);
    dropped = dropCount;
  }
  queue.push(item);
  return dropped;
}
