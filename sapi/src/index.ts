/**
 * @sfmc-bds/module-activity-log — 原生事件 + record/query 审计中枢
 */

import {
  Block,
  Entity,
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
  type EntitySpawnAfterEvent,
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
import { config } from "@sfmc-bds/sdk/sapi/config";
import { db } from "@sfmc-bds/sdk/sapi/db";
import { ModuleRegistry } from "@sfmc-bds/sdk/module-loader";
import { debug } from "@sfmc-bds/sdk/sapi/runtime";
import { service } from "@sfmc-bds/sdk/sapi/service";

const MODULE_ID = "activity-log";
const TABLE = "sfmc_activities";

type Level = "info" | "warn" | "error";

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

interface ActivityRecord {
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

let queue: QueueEntry[] = [];
let retentionDays = 30;
let flushIntervalMs = 2000;
let batchSize = 100;
let flushTimer: number | undefined;
let cleanupTimer: number | undefined;
const unprovide: Array<() => void> = [];
const eventCleanups: Array<() => void> = [];

type EventSignal<T> = {
  subscribe: (cb: (arg: T) => void) => (arg: T) => void;
  unsubscribe: (cb: (arg: T) => void) => void;
};

function dimId(entityOrBlock: Entity | Block): string {
  try {
    return entityOrBlock.dimension?.id || "";
  } catch {
    return "";
  }
}

function loc(v?: Vector3): [number | null, number | null, number | null] {
  if (!v) return [null, null, null];
  return [Math.round(v.x), Math.round(v.y), Math.round(v.z)];
}

function enqueue(partial: {
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
}): void {
  const now = Date.now();
  queue.push({
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
  });
  if (queue.length >= batchSize) void flush();
}

async function flush(): Promise<void> {
  if (queue.length === 0) return;
  const batch = queue;
  queue = [];
  try {
    await db.tx(async (tx) => {
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
  } catch (err) {
    debug.e(
      "ActivityLog",
      `flush failed (${batch.length} retained)`,
      err instanceof Error ? err : new Error(String(err)),
    );
    queue = batch.concat(queue);
  }
}

async function cleanupExpired(): Promise<void> {
  const cutoff = Date.now() - retentionDays * 86400_000;
  try {
    const old = await db.query<{ id: string }>(TABLE, {
      where: { lt: ["timestamp", cutoff] },
      limit: 1000,
    });
    if (old.length === 0) return;
    await db.tx(async (tx) => {
      for (const row of old) await tx.delete(TABLE, row.id);
    });
  } catch (err) {
    debug.e("ActivityLog", "cleanup failed", err instanceof Error ? err : new Error(String(err)));
  }
}

function handleRecord(input: Record<string, unknown>): { ok: boolean } {
  const eventType = String(input.eventType ?? "");
  if (!eventType) return { ok: false };
  const levelRaw = String(input.level ?? "info");
  const level: Level = levelRaw === "warn" || levelRaw === "error" ? levelRaw : "info";
  enqueue({
    eventType,
    actorId: typeof input.actorId === "string" ? input.actorId : undefined,
    actorName: typeof input.actorName === "string" ? input.actorName : undefined,
    targetId: typeof input.targetId === "string" ? input.targetId : undefined,
    dimension: typeof input.dimension === "string" ? input.dimension : undefined,
    x: typeof input.x === "number" ? input.x : undefined,
    y: typeof input.y === "number" ? input.y : undefined,
    z: typeof input.z === "number" ? input.z : undefined,
    level,
    payload:
      input.payload && typeof input.payload === "object"
        ? (input.payload as Record<string, unknown>)
        : undefined,
  });
  return { ok: true };
}

async function handleQuery(input: Record<string, unknown>): Promise<{
  records: ActivityRecord[];
  total: number;
}> {
  const limit = Math.min(100, Math.max(1, Number(input.limit) || 20));
  const offset = Math.max(0, Number(input.offset) || 0);
  const clauses: Array<Record<string, unknown>> = [];
  if (typeof input.targetId === "string" && input.targetId) {
    clauses.push({ eq: ["target_id", input.targetId] });
  }
  if (typeof input.actorId === "string" && input.actorId) {
    clauses.push({ eq: ["actor_id", input.actorId] });
  }
  if (typeof input.eventTypePrefix === "string" && input.eventTypePrefix) {
    clauses.push({ like: ["event_type", `${input.eventTypePrefix}%`] });
  }
  if (typeof input.from === "number") clauses.push({ gte: ["timestamp", input.from] });
  if (typeof input.to === "number") clauses.push({ lte: ["timestamp", input.to] });

  const where =
    clauses.length === 0 ? undefined : clauses.length === 1 ? clauses[0] : { and: clauses };

  const rows = await db.query<Record<string, unknown>>(TABLE, {
    ...(where ? { where: where as never } : {}),
    orderBy: { field: "timestamp", dir: "desc" },
    limit,
    offset,
  });

  // 无 count API 时用同条件再拉一页上限估算；此处返回本页长度作为近似 total 下限
  const countRows = await db.query<{ id: string }>(TABLE, {
    ...(where ? { where: where as never } : {}),
    limit: 10000,
  });

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

  return { records, total: countRows.length };
}

function playerEnqueue(player: Player, eventType: string, extra: Partial<Parameters<typeof enqueue>[0]> = {}): void {
  const [x, y, z] = loc(player.location);
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

function subscribeNative(): void {
  const AE = world.afterEvents;

  safeSubscribe(AE.playerSpawn, (event: PlayerSpawnAfterEvent) => {
    if (event.initialSpawn) playerEnqueue(event.player, "player.join");
    else playerEnqueue(event.player, "player.spawn");
  });

  safeSubscribe(AE.playerLeave, (event: PlayerLeaveAfterEvent) => {
    enqueue({
      eventType: "player.leave",
      actorId: event.playerId,
      actorName: event.playerName,
      payload: { playerId: event.playerId },
    });
  });

  safeSubscribe(AE.playerDimensionChange, (event: PlayerDimensionChangeAfterEvent) => {
    const [x, y, z] = loc(event.toLocation);
    playerEnqueue(event.player, "player.dimension", {
      x,
      y,
      z,
      payload: {
        from: event.fromDimension.id,
        to: event.toDimension.id,
      },
    });
  });

  safeSubscribe(AE.playerGameModeChange, (event: PlayerGameModeChangeAfterEvent) => {
    playerEnqueue(event.player, "player.gamemode", {
      payload: { from: event.fromGameMode, to: event.toGameMode },
    });
  });

  safeSubscribe(AE.chatSend, (event: ChatSendAfterEvent) => {
    playerEnqueue(event.sender, "player.chat", {
      payload: { message: event.message },
    });
  });

  safeSubscribe(AE.playerBreakBlock, (event: PlayerBreakBlockAfterEvent) => {
    const [x, y, z] = loc(event.block.location);
    playerEnqueue(event.player, "block.break", {
      targetId: event.brokenBlockPermutation.type.id,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.playerPlaceBlock, (event: PlayerPlaceBlockAfterEvent) => {
    const [x, y, z] = loc(event.block.location);
    playerEnqueue(event.player, "block.place", {
      targetId: event.block.typeId,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.entityDie, (event: EntityDieAfterEvent) => {
    const dead = event.deadEntity;
    const [x, y, z] = loc(dead.location);
    const killer = event.damageSource.damagingEntity;
    if (killer?.typeId === "minecraft:player") {
      playerEnqueue(killer as Player, "entity.death", {
        targetId: dead.typeId === "minecraft:player" ? (dead as Player).id : dead.typeId,
        x,
        y,
        z,
        payload: { cause: event.damageSource.cause },
      });
    } else {
      enqueue({
        eventType: "entity.death",
        actorName: killer?.typeId || event.damageSource.cause,
        targetId: dead.typeId,
        dimension: dimId(dead),
        x,
        y,
        z,
        payload: { cause: event.damageSource.cause },
      });
    }
  });

  safeSubscribe(AE.entityHitEntity, (event: EntityHitEntityAfterEvent) => {
    const attacker = event.damagingEntity;
    const victim = event.hitEntity;
    const [x, y, z] = loc(victim.location);
    if (attacker.typeId === "minecraft:player") {
      playerEnqueue(attacker as Player, "entity.hit", {
        targetId: victim.typeId === "minecraft:player" ? (victim as Player).id : victim.typeId,
        x,
        y,
        z,
      });
    }
  });

  safeSubscribe(AE.entityHurt, (event: EntityHurtAfterEvent) => {
    if (event.hurtEntity.typeId !== "minecraft:player") return;
    playerEnqueue(event.hurtEntity as Player, "entity.hurt", {
      payload: {
        damage: event.damage,
        cause: event.damageSource.cause,
      },
    });
  });

  safeSubscribe(AE.playerInteractWithEntity, (event: PlayerInteractWithEntityAfterEvent) => {
    const [x, y, z] = loc(event.target.location);
    playerEnqueue(event.player, "entity.interact", {
      targetId: event.target.typeId,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.entityTamed, (event: EntityTamedAfterEvent) => {
    const tamer = event.tamingEntity;
    if (!tamer || tamer.typeId !== "minecraft:player") return;
    const [x, y, z] = loc(event.entity.location);
    playerEnqueue(tamer as Player, "entity.tame", {
      targetId: event.entity.typeId,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.entitySpawn, (event: EntitySpawnAfterEvent) => {
    if (event.entity.typeId === "minecraft:player") return;
    const [x, y, z] = loc(event.entity.location);
    enqueue({
      eventType: "entity.spawn",
      actorName: event.entity.typeId,
      dimension: dimId(event.entity),
      x,
      y,
      z,
      payload: { cause: event.cause },
    });
  });

  safeSubscribe(AE.entityItemDrop, (event: EntityItemDropAfterEvent) => {
    const e = event.entity;
    const items = event.items.map((item: Entity) => item.typeId);
    if (e.typeId === "minecraft:player") {
      playerEnqueue(e as Player, "item.drop", { payload: { items } });
    } else {
      const [x, y, z] = loc(e.location);
      enqueue({
        eventType: "item.drop",
        actorName: e.typeId,
        dimension: dimId(e),
        x,
        y,
        z,
        payload: { items },
      });
    }
  });

  safeSubscribe(AE.entityItemPickup, (event: EntityItemPickupAfterEvent) => {
    if (event.entity.typeId !== "minecraft:player") return;
    playerEnqueue(event.entity as Player, "item.pickup", {
      payload: { items: event.items.map((item: ItemStack) => item.type.id) },
    });
  });

  safeSubscribe(AE.blockContainerOpened, (event: BlockContainerOpenedAfterEvent) => {
    const source = event.openSource.entity;
    if (!source || source.typeId !== "minecraft:player") return;
    const [x, y, z] = loc(event.block.location);
    playerEnqueue(source as Player, "container.open", {
      targetId: event.block.typeId,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.blockContainerClosed, (event: BlockContainerClosedAfterEvent) => {
    const source = event.closeSource.entity;
    if (!source || source.typeId !== "minecraft:player") return;
    const [x, y, z] = loc(event.block.location);
    playerEnqueue(source as Player, "container.close", {
      targetId: event.block.typeId,
      x,
      y,
      z,
    });
  });

  safeSubscribe(AE.explosion, (event: ExplosionAfterEvent) => {
    const source = event.source;
    const [x, y, z] = source ? loc(source.location) : [null, null, null];
    enqueue({
      eventType: "world.explosion",
      actorId: source?.typeId === "minecraft:player" ? (source as Player).id : "",
      actorName: source?.typeId || "unknown",
      dimension: event.dimension.id,
      x,
      y,
      z,
      level: "warn",
      payload: { impactedBlocks: event.getImpactedBlocks().length },
    });
  });
}

ModuleRegistry.register({
  id: MODULE_ID,
  afterWorldLoad: false,
  lifecycle: {
    registerPermissions() {
      // 无命令面
    },
    registerCommands() {
      // 无
    },
    registerEvents() {
      subscribeNative();
    },
    async init() {
      const retention = await config.get<number>("retention_days");
      const flushMs = await config.get<number>("flush_interval_ms");
      const batch = await config.get<number>("batch_size");
      if (typeof retention === "number" && retention > 0) retentionDays = retention;
      if (typeof flushMs === "number" && flushMs > 0) flushIntervalMs = flushMs;
      if (typeof batch === "number" && batch > 0) batchSize = batch;

      await db.defineTable(TABLE, {
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
      // 约每日清理一次（72000 ticks ≈ 1 小时检查，内部按 retention 删）
      cleanupTimer = system.runInterval(() => void cleanupExpired(), 72000);

      unprovide.push(service.provide("activity.record", (input) => handleRecord(input)));
      unprovide.push(service.provide("activity.query", (input) => handleQuery(input)));

      debug.i("ActivityLog", `init flushMs=${flushIntervalMs} batch=${batchSize}`);
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
      if (cleanupTimer !== undefined) {
        try {
          system.clearRun(cleanupTimer);
        } catch {
          /* ignore */
        }
        cleanupTimer = undefined;
      }
      void flush();
      debug.i("ActivityLog", "cleanup");
    },
  },
});
