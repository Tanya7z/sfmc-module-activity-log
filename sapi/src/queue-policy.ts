/**
 * 从待刷盘队列中取出一个有界批次，避免单次事务长期独占数据库连接。
 */
export function takeBatch<T>(queue: T[], batchSize: number): T[] {
  return queue.splice(0, Math.max(1, Math.floor(batchSize)));
}
