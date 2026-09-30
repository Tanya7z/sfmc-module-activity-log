/**
 * @sfmc-bds/module-activity-log
 *
 * BDS SAPI 行为日志模块：
 * 1. 监听并捕获核心游戏行为（进出服、聊天、方块破坏/放置、生物击杀/死亡/受击、容器、掉落物、爆炸等）；
 * 2. 内存队列缓冲 + 有界容量控制 + 定时批量事务写入平台数据库；
 * 3. 对外提供 activity.record 与 activity.query 服务。
 */

import {
  Block,
  Entity,
  EntityItemComponent,
  ItemStack,
  Player,
  system,
  Vector3,
  world,
  type BlockContainerClosedAfterEvent,
  type BlockContainerOpenedAfterEvent,
  type ChatSendAfterEvent,
  type EntityDieAfterEvent,
  type EntityHitEntityAfterEvent,
  type EntityHurtAfterEvent,
  type EntityItemDropAfterEvent,
  type EntityItemPickupAfterEvent,
  type EntityTamedAfterEvent,
  type ExplosionAfterEvent,
  type PlayerBreakBlockAfterEvent,
  type PlayerDimensionChangeAfterEvent,
  type PlayerGameModeChangeAfterEvent,
  type PlayerInteractWithEntityAfterEvent,
  type PlayerLeaveAfterEvent,
  type PlayerPlaceBlockAfterEvent,
  type PlayerSpawnAfterEvent,
} from "@minecraft/server";
import { ModuleRegistry, type ModuleServices } from "@sfmc-bds/sdk/module-loader";
import { config as globalConfig } from "@sfmc-bds/sdk/sapi/config";
import { db as globalDb } from "@sfmc-bds/sdk/sapi/db";
import { debug } from "@sfmc-bds/sdk/sapi/runtime";
import { service as globalService } from "@sfmc-bds/sdk/sapi/service";
import { DEFAULT_MAX_QUEUE_SIZE, safeEnqueue, takeBatch } from "./queue-policy.js";

// ==========================================
// 1. 常量与类型定义
// ==========================================

const MODULE_ID = "activity-log";
const TABLE = "activities";

export type Level = "info" | "warn" | "error";

// 内部队列数据
interface QueueEntry {
  timestamp: number;
  event_type: string;
  actor_id: string;
  actor_name: string;
  target_id: string;
  dimension: string;
  x: number | null;
  y: number | null;
  z: number | null;
  level: Level;
  payload_json: string;
  created_at: number;
}

// 对外返回查询记录
export interface ActivityRecord {
  id: number | string;
  timestamp: number;
  eventType: string;
  actorId?: string;
  actorName?: string;
  targetId?: string;
  dimension?: string;
  x?: number;
  y?: number;
  z?: number;
  level: Level;
  payload?: Record<string, unknown>;
  createdAt: string;
}

// 调用方提交的新日志
export interface ActivityRecordInput {
  eventType: string;
  actorId?: string;
  actorName?: string;
  targetId?: string;
  dimension?: string;
  x?: number | null;
  y?: number | null;
  z?: number | null;
  level?: Level;
  payload?: Record<string, unknown>;
}

// 查询条件
export interface ActivityQueryInput {
  eventType?: string;
  eventTypePrefix?: string;
  actorId?: string;
  targetId?: string;
  dimension?: string;
  level?: Level;
  from?: number;
  to?: number;
  limit?: number;
  offset?: number;
}

// ==========================================
// 2. 运行时状态与上下文
// ==========================================

let activeDb = globalDb;
let activeConfig = globalConfig;
let activeService = globalService;

let queue: QueueEntry[] = [];
let flushIntervalMs = 2000;
let batchSize = 50;
let maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
let flushTimer: number | undefined;
let flushInProgress = false;
let lastFlushErrorTime = 0;
const FLUSH_ERROR_COOLDOWN_MS = 5000;

const unprovide: Array<() => void> = [];
const eventCleanups: Array<() => void> = [];

type EventSignal<T> = {
  subscribe: (cb: (arg: T) => void) => (arg: T) => void;
  unsubscribe: (cb: (arg: T) => void) => void;
};

// ==========================================
// 3. SAPI 辅助与安全检测
// ==========================================

/**
 * 安全检测 SAPI 实体/方块是否仍然有效且处于加载状态。
 * 在 SAPI 规范中，isValid 是 boolean getter 属性；本函数兼顾极端情况下的方法调用。
 */
function isSapiValid(target: unknown): boolean {
  if (!target || typeof target !== "object") return false;
  if ("isValid" in target) {
    const valid = (target as { isValid?: unknown }).isValid;
    if (typeof valid === "boolean") return valid;
    if (typeof valid === "function") {
      try {
        return Boolean(valid.call(target));
      } catch {
        return false;
      }
    }
  }
  return true;
}

function dimId(target?: Entity | Block | null): string {
  if (!target || !isSapiValid(target)) return "";
  try {
    return target.dimension?.id || "";
  } catch {
    return "";
  }
}

// 获取坐标
function safeLoc(target?: Entity | Block | Vector3 | null): [number | null, number | null, number | null] {
  if (!target) return [null, null, null];
  try {
    if ("x" in target && typeof target.x === "number") {
      return [Math.round(target.x), Math.round(target.y), Math.round(target.z)];
    }
    if (!isSapiValid(target)) return [null, null, null];
    const loc = (target as Entity | Block).location;
    if (loc && typeof loc.x === "number") {
      return [Math.round(loc.x), Math.round(loc.y), Math.round(loc.z)];
    }
  } catch {
    return [null, null, null];
  }
  return [null, null, null];
}

function safeTypeId(target?: Entity | Block | null): string {
  if (!target || !isSapiValid(target)) return "";
  try {
    return (target as { typeId?: string }).typeId || "";
  } catch {
    return "";
  }
}

// ==========================================
// 4. 队列缓冲与刷盘
// ==========================================

function enqueue(partial: ActivityRecordInput): void {
  const now = Date.now();
  const entry: QueueEntry = {
    timestamp: now,
    event_type: partial.eventType,
    actor_id: partial.actorId ?? "",
    actor_name: partial.actorName ?? "",
    target_id: partial.targetId ?? "",
    dimension: partial.dimension ?? "",
    x: partial.x ?? null,
    y: partial.y ?? null,
    z: partial.z ?? null,
    level: partial.level ?? "info",
    payload_json: JSON.stringify(partial.payload ?? {}),
    created_at: now,
  };

  const dropped = safeEnqueue(queue, entry, maxQueueSize);
  if (dropped > 0) {
    debug.w("ActivityLog", `Queue overflow: dropped ${dropped} oldest entries.`);
  }

  // 积压达到批次大小且不在失败冷却期时，尝试立即刷盘
  if (queue.length >= batchSize && Date.now() - lastFlushErrorTime >= FLUSH_ERROR_COOLDOWN_MS) {
    void flush();
  }
}

async function flush(): Promise<void> {
  if (flushInProgress || queue.length === 0) return;
  // 失败冷却期内避免高频重试压垮服务器
  if (Date.now() - lastFlushErrorTime < FLUSH_ERROR_COOLDOWN_MS) return;

  flushInProgress = true;
  const batch = takeBatch(queue, batchSize);
  let success = false;

  try {
    await activeDb.tx(async (tx) => {
      for (const e of batch) {
        await tx.insert(TABLE, {
          id: `${e.timestamp}_${Math.random().toString(36).slice(2, 8)}`,
          timestamp: e.timestamp,
          event_type: e.event_type,
          actor_id: e.actor_id,
          actor_name: e.actor_name,
          target_id: e.target_id,
          dimension: e.dimension,
          x: e.x,
          y: e.y,
          z: e.z,
          level: e.level,
          payload_json: e.payload_json,
          created_at: e.created_at,
        });
      }
    });
    success = true;
    lastFlushErrorTime = 0;
  } catch (err) {
    lastFlushErrorTime = Date.now();
    debug.e(
      "ActivityLog",
      `flush failed (${batch.length} retained, cooling down ${FLUSH_ERROR_COOLDOWN_MS}ms)`,
      err instanceof Error ? err : new Error(String(err))
    );
    // 失败批次放回队列头部（并受容量上限约束保护）
    queue = batch.concat(queue);
    if (queue.length > maxQueueSize) {
      queue.splice(maxQueueSize);
    }
  } finally {
    flushInProgress = false;
    // 仅在上次刷盘成功且仍有积压数据时，继续刷下一批，彻底杜绝死循环
    if (success && queue.length >= batchSize) {
      void flush();
    }
  }
}

function playerEnqueue(player: Player, eventType: string, extra: Partial<ActivityRecordInput> = {}): void {
  try {
    if (!player || !isSapiValid(player)) return;
    const [x, y, z] = safeLoc(player);
    enqueue({
      eventType,
      actorId: player.id,
      actorName: player.name,
      dimension: dimId(player),
      x,
      y,
      z,
      ...extra,
    });
  } catch (err) {
    debug.w("ActivityLog", `playerEnqueue failed for ${eventType}`, err);
  }
}

function safeSubscribe<T>(signal: EventSignal<T> | undefined, cb: (arg: T) => void): void {
  if (!signal || typeof signal.subscribe !== "function") return;
  const subscribed = signal.subscribe(cb);
  eventCleanups.push(() => {
    try {
      signal.unsubscribe(subscribed);
    } catch {
      /* ignore */
    }
  });
}

// ==========================================
// 5. 服务接口
// ==========================================

function handleRecord(input: unknown): { ok: boolean } {
  if (!input || typeof input !== "object") return { ok: false };
  const raw = input as Record<string, unknown>;
  const eventType = String(raw.eventType ?? "").trim();
  if (!eventType) return { ok: false };

  const levelRaw = String(raw.level ?? "info");
  const level: Level = levelRaw === "warn" || levelRaw === "error" ? levelRaw : "info";

  enqueue({
    eventType,
    actorId: typeof raw.actorId === "string" ? raw.actorId : undefined,
    actorName: typeof raw.actorName === "string" ? raw.actorName : undefined,
    targetId: typeof raw.targetId === "string" ? raw.targetId : undefined,
    dimension: typeof raw.dimension === "string" ? raw.dimension : undefined,
    x: typeof raw.x === "number" ? raw.x : undefined,
    y: typeof raw.y === "number" ? raw.y : undefined,
    z: typeof raw.z === "number" ? raw.z : undefined,
    level,
    payload:
      raw.payload && typeof raw.payload === "object" && !Array.isArray(raw.payload)
        ? (raw.payload as Record<string, unknown>)
        : undefined,
  });
  return { ok: true };
}

async function handleQuery(input: unknown): Promise<{
  records: ActivityRecord[];
  total: number;
}> {
  if (!input || typeof input !== "object") {
    return { records: [], total: 0 };
  }
  const raw = input as Record<string, unknown>;
  const limit = Math.min(100, Math.max(1, Number(raw.limit) || 20));
  const offset = Math.max(0, Number(raw.offset) || 0);

  const clauses: Array<Record<string, unknown>> = [];

  if (typeof raw.eventType === "string" && raw.eventType.trim()) {
    clauses.push({ eq: ["event_type", raw.eventType.trim()] });
  } else if (typeof raw.eventTypePrefix === "string" && raw.eventTypePrefix.trim()) {
    clauses.push({ like: ["event_type", `${raw.eventTypePrefix.trim()}%`] });
  }

  if (typeof raw.targetId === "string" && raw.targetId) {
    clauses.push({ eq: ["target_id", raw.targetId] });
  }
  if (typeof raw.actorId === "string" && raw.actorId) {
    clauses.push({ eq: ["actor_id", raw.actorId] });
  }
  if (typeof raw.dimension === "string" && raw.dimension) {
    clauses.push({ eq: ["dimension", raw.dimension] });
  }
  if (typeof raw.level === "string" && raw.level) {
    clauses.push({ eq: ["level", raw.level] });
  }
  if (typeof raw.from === "number") clauses.push({ gte: ["timestamp", raw.from] });
  if (typeof raw.to === "number") clauses.push({ lte: ["timestamp", raw.to] });

  const where = clauses.length === 0 ? undefined : clauses.length === 1 ? clauses[0] : { and: clauses };

  const rows = await activeDb.query<Record<string, unknown>>(TABLE, {
    ...(where ? { where: where as never } : {}),
    orderBy: { field: "timestamp", dir: "desc" },
    limit,
    offset,
  });

  // 分页总数优化：若当前页数量小于 limit，说明已到尾页，总数精确为 offset + 当前行数；
  const total = rows.length < limit ? offset + rows.length : offset + rows.length + 1;

  const records: ActivityRecord[] = rows.map((r) => {
    let payload: Record<string, unknown> | undefined;
    try {
      payload = JSON.parse(String(r.payload_json || "{}")) as Record<string, unknown>;
    } catch {
      payload = undefined;
    }
    const rec: ActivityRecord = {
      id: (r.id as string | number) ?? "",
      timestamp: Number(r.timestamp) || 0,
      eventType: String(r.event_type ?? ""),
      level: (r.level as Level) || "info",
      createdAt: String(r.created_at ?? ""),
    };
    if (r.actor_id) rec.actorId = String(r.actor_id);
    if (r.actor_name) rec.actorName = String(r.actor_name);
    if (r.target_id) rec.targetId = String(r.target_id);
    if (r.dimension) rec.dimension = String(r.dimension);
    if (typeof r.x === "number") rec.x = r.x;
    if (typeof r.y === "number") rec.y = r.y;
    if (typeof r.z === "number") rec.z = r.z;
    if (payload) rec.payload = payload;
    return rec;
  });

  return { records, total };
}

// ==========================================
// 6. 原生事件监听绑定
// ==========================================

function subscribeNative(): void {
  const AE = world.afterEvents;

  // 1. 玩家加入 / 生成
  safeSubscribe(AE.playerSpawn, (event: PlayerSpawnAfterEvent) => {
    try {
      if (event.initialSpawn) playerEnqueue(event.player, "player.join");
      else playerEnqueue(event.player, "player.spawn");
    } catch (err) {
      debug.w("ActivityLog", "playerSpawn handler failed", err);
    }
  });

  // 2. 玩家离开
  safeSubscribe(AE.playerLeave, (event: PlayerLeaveAfterEvent) => {
    try {
      enqueue({
        eventType: "player.leave",
        actorId: event.playerId,
        actorName: event.playerName,
        payload: { playerId: event.playerId },
      });
    } catch (err) {
      debug.w("ActivityLog", "playerLeave handler failed", err);
    }
  });

  // 3. 维度切换
  safeSubscribe(AE.playerDimensionChange, (event: PlayerDimensionChangeAfterEvent) => {
    try {
      const [x, y, z] = safeLoc(event.toLocation);
      const toDim = event.toDimension?.id || "";
      const fromDim = event.fromDimension?.id || "";
      playerEnqueue(event.player, "player.dimension", {
        dimension: toDim,
        x,
        y,
        z,
        payload: { from: fromDim, to: toDim },
      });
    } catch (err) {
      debug.w("ActivityLog", "playerDimensionChange handler failed", err);
    }
  });

  // 4. 游戏模式切换
  safeSubscribe(AE.playerGameModeChange, (event: PlayerGameModeChangeAfterEvent) => {
    try {
      playerEnqueue(event.player, "player.gamemode", {
        payload: { from: event.fromGameMode, to: event.toGameMode },
      });
    } catch (err) {
      debug.w("ActivityLog", "playerGameModeChange handler failed", err);
    }
  });

  // 5. 聊天记录
  safeSubscribe(AE.chatSend, (event: ChatSendAfterEvent) => {
    try {
      playerEnqueue(event.sender, "player.chat", {
        payload: { message: event.message },
      });
    } catch (err) {
      debug.w("ActivityLog", "chatSend handler failed", err);
    }
  });

  // 6. 破坏方块
  safeSubscribe(AE.playerBreakBlock, (event: PlayerBreakBlockAfterEvent) => {
    try {
      const [x, y, z] = safeLoc(event.block);
      playerEnqueue(event.player, "block.break", {
        targetId: event.brokenBlockPermutation?.type?.id || "",
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "playerBreakBlock handler failed", err);
    }
  });

  // 7. 放置方块
  safeSubscribe(AE.playerPlaceBlock, (event: PlayerPlaceBlockAfterEvent) => {
    try {
      const [x, y, z] = safeLoc(event.block);
      playerEnqueue(event.player, "block.place", {
        targetId: safeTypeId(event.block),
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "playerPlaceBlock handler failed", err);
    }
  });

  // 8. 击杀与死亡 (独立判定，PvP 双方完整记录)
  safeSubscribe(AE.entityDie, (event: EntityDieAfterEvent) => {
    try {
      const dead = event.deadEntity;
      const killer = event.damageSource?.damagingEntity;
      const killerTypeId = safeTypeId(killer);
      const deadTypeId = safeTypeId(dead);
      const cause = event.damageSource?.cause;

      // 击杀者是玩家 -> 记录击杀事件 (entity.kill)
      if (killerTypeId === "minecraft:player" && killer) {
        const deadTargetId = deadTypeId === "minecraft:player" ? (dead as Player).id : deadTypeId;
        playerEnqueue(killer as Player, "entity.kill", {
          targetId: deadTargetId,
          payload: { cause, victimType: deadTypeId },
        });
      }

      // 死者是玩家 -> 记录死亡事件 (entity.death)，即使被玩家击杀也完整记录
      if (deadTypeId === "minecraft:player" && dead) {
        const [deadX, deadY, deadZ] = safeLoc(dead);
        const killerTargetId = killerTypeId === "minecraft:player" ? (killer as Player).id : killerTypeId;
        playerEnqueue(dead as Player, "entity.death", {
          targetId: killerTargetId || cause || "unknown",
          x: deadX ?? undefined,
          y: deadY ?? undefined,
          z: deadZ ?? undefined,
          payload: { cause, killer: killerTypeId || undefined },
        });
      }
    } catch (err) {
      debug.w("ActivityLog", "entityDie handler failed", err);
    }
  });

  // 9. 实体打击命中
  safeSubscribe(AE.entityHitEntity, (event: EntityHitEntityAfterEvent) => {
    try {
      const attacker = event.damagingEntity;
      const victim = event.hitEntity;
      const [x, y, z] = safeLoc(victim);
      const attackerTypeId = safeTypeId(attacker);
      const victimTypeId = safeTypeId(victim);
      if (attackerTypeId === "minecraft:player" && attacker) {
        playerEnqueue(attacker as Player, "entity.hit", {
          targetId: victimTypeId === "minecraft:player" ? (victim as Player).id : victimTypeId,
          x,
          y,
          z,
        });
      }
    } catch (err) {
      debug.w("ActivityLog", "entityHitEntity handler failed", err);
    }
  });

  // 10. 玩家受到伤害
  safeSubscribe(AE.entityHurt, (event: EntityHurtAfterEvent) => {
    try {
      if (safeTypeId(event.hurtEntity) !== "minecraft:player") return;
      const damaging = event.damageSource?.damagingEntity;
      const damagingTypeId = safeTypeId(damaging);
      playerEnqueue(event.hurtEntity as Player, "entity.hurt", {
        targetId: damagingTypeId || undefined,
        payload: {
          damage: event.damage,
          cause: event.damageSource?.cause,
          damagingEntity: damagingTypeId || undefined,
        },
      });
    } catch (err) {
      debug.w("ActivityLog", "entityHurt handler failed", err);
    }
  });

  // 11. 玩家与实体交互
  safeSubscribe(AE.playerInteractWithEntity, (event: PlayerInteractWithEntityAfterEvent) => {
    try {
      const [x, y, z] = safeLoc(event.target);
      playerEnqueue(event.player, "entity.interact", {
        targetId: safeTypeId(event.target),
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "playerInteractWithEntity handler failed", err);
    }
  });

  // 12. 驯服实体
  safeSubscribe(AE.entityTamed, (event: EntityTamedAfterEvent) => {
    try {
      const tamer = event.tamingEntity;
      if (!tamer || safeTypeId(tamer) !== "minecraft:player") return;
      const [x, y, z] = safeLoc(event.entity);
      playerEnqueue(tamer as Player, "entity.tame", {
        targetId: safeTypeId(event.entity),
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "entityTamed handler failed", err);
    }
  });

  // 13. 扔出物品 (从 EntityItemComponent 读取具体物品 ID)
  safeSubscribe(AE.entityItemDrop, (event: EntityItemDropAfterEvent) => {
    try {
      const e = event.entity;
      if (safeTypeId(e) !== "minecraft:player") return;
      const items = (event.items || [])
        .map((itemEntity: Entity) => {
          if (!isSapiValid(itemEntity)) return "";
          try {
            const comp = itemEntity.getComponent("minecraft:item") as EntityItemComponent | undefined;
            return comp?.itemStack?.typeId || "";
          } catch {
            return "";
          }
        })
        .filter(Boolean);
      playerEnqueue(e as Player, "item.drop", { payload: { items } });
    } catch (err) {
      debug.w("ActivityLog", "entityItemDrop handler failed", err);
    }
  });

  // 14. 捡起物品
  safeSubscribe(AE.entityItemPickup, (event: EntityItemPickupAfterEvent) => {
    try {
      if (safeTypeId(event.entity) !== "minecraft:player") return;
      const items = (event.items || []).map((item: ItemStack) => item?.typeId || item?.type?.id || "").filter(Boolean);
      playerEnqueue(event.entity as Player, "item.pickup", {
        payload: { items },
      });
    } catch (err) {
      debug.w("ActivityLog", "entityItemPickup handler failed", err);
    }
  });

  // 15. 打开容器
  safeSubscribe(AE.blockContainerOpened, (event: BlockContainerOpenedAfterEvent) => {
    try {
      const source = event.openSource?.entity;
      if (!source || safeTypeId(source) !== "minecraft:player") return;
      const [x, y, z] = safeLoc(event.block);
      playerEnqueue(source as Player, "container.open", {
        targetId: safeTypeId(event.block),
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "blockContainerOpened handler failed", err);
    }
  });

  // 16. 关闭容器
  safeSubscribe(AE.blockContainerClosed, (event: BlockContainerClosedAfterEvent) => {
    try {
      const source = event.closeSource?.entity;
      if (!source || safeTypeId(source) !== "minecraft:player") return;
      const [x, y, z] = safeLoc(event.block);
      playerEnqueue(source as Player, "container.close", {
        targetId: safeTypeId(event.block),
        x,
        y,
        z,
      });
    } catch (err) {
      debug.w("ActivityLog", "blockContainerClosed handler failed", err);
    }
  });

  // 17. 爆炸事件 (包含 TNT、苦力怕、水晶及玩家爆炸)
  safeSubscribe(AE.explosion, (event: ExplosionAfterEvent) => {
    try {
      const source = event.source;
      const sourceTypeId = safeTypeId(source);
      const [x, y, z] = safeLoc(source);
      const impacted = event.getImpactedBlocks ? event.getImpactedBlocks() : [];
      const dimension = event.dimension?.id || "";

      if (sourceTypeId === "minecraft:player" && source) {
        playerEnqueue(source as Player, "world.explosion", {
          x,
          y,
          z,
          level: "warn",
          payload: { impactedBlocks: impacted ? impacted.length : 0 },
        });
      } else {
        enqueue({
          eventType: "world.explosion",
          actorId: sourceTypeId || "unknown",
          actorName: sourceTypeId || "explosion",
          dimension,
          x,
          y,
          z,
          level: "warn",
          payload: {
            source: sourceTypeId || undefined,
            impactedBlocks: impacted ? impacted.length : 0,
          },
        });
      }
    } catch (err) {
      debug.w("ActivityLog", "explosion handler failed", err);
    }
  });
}

// ==========================================
// 7. 模块生命周期注册
// ==========================================

ModuleRegistry.register({
  id: MODULE_ID,
  afterWorldLoad: false,
  lifecycle: {
    registerPermissions() {},
    registerEvents() { subscribeNative() },
    async init(services?: ModuleServices) {
      if (services?.db) activeDb = services.db;
      if (services?.config) activeConfig = services.config;
      if (services?.service) activeService = services.service;

      const flushMs = await activeConfig.get<number>("flush_interval_ms");
      const batch = await activeConfig.get<number>("batch_size");
      const maxQueue = await activeConfig.get<number>("max_queue_size");

      if (typeof flushMs === "number" && flushMs > 0) flushIntervalMs = flushMs;
      if (typeof batch === "number" && batch > 0) batchSize = batch;
      if (typeof maxQueue === "number" && maxQueue > 0) maxQueueSize = maxQueue;

      await activeDb.defineTable(TABLE, {
        id: { type: "TEXT", primary: true },
        timestamp: { type: "INTEGER", notNull: true, index: true },
        event_type: { type: "TEXT", notNull: true, index: true },
        actor_id: { type: "TEXT", default: "", index: true },
        actor_name: { type: "TEXT", default: "" },
        target_id: { type: "TEXT", default: "", index: true },
        dimension: { type: "TEXT", default: "" },
        x: { type: "INTEGER" },
        y: { type: "INTEGER" },
        z: { type: "INTEGER" },
        level: { type: "TEXT", default: "info" },
        payload_json: { type: "TEXT", default: "{}" },
        created_at: { type: "INTEGER", notNull: true },
      });

      const flushTicks = Math.max(1, Math.round(flushIntervalMs / 50));
      flushTimer = system.runInterval(() => void flush(), flushTicks);

      unprovide.push(activeService.provide("activity.record", (input) => handleRecord(input)));
      unprovide.push(activeService.provide("activity.query", (input) => handleQuery(input)));

      debug.i("ActivityLog", `init flushMs=${flushIntervalMs} batch=${batchSize} maxQueue=${maxQueueSize}`);
    },
    cleanup() {
      for (const off of unprovide.splice(0, unprovide.length)) {
        try {
          off();
        } catch {
          /* ignore */
        }
      }
      for (const c of eventCleanups.splice(0, eventCleanups.length)) c();
      if (flushTimer !== undefined) {
        try {
          system.clearRun(flushTimer);
        } catch {
          /* ignore */
        }
        flushTimer = undefined;
      }
      void flush();
      debug.i("ActivityLog", "cleanup");
    },
  },
});
