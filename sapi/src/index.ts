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
import { ModuleRegistry } from "@sfmc-bds/sdk/module-loader";
import { config } from "@sfmc-bds/sdk/sapi/config";
import { db } from "@sfmc-bds/sdk/sapi/db";
import { debug } from "@sfmc-bds/sdk/sapi/runtime";
import { service } from "@sfmc-bds/sdk/sapi/service";
import { takeBatch } from "./queue-policy.js";

const MODULE_ID = "activity-log";
const TABLE = "sfmc_activities";

type Level = "info" | "warn" | "error";

// === TYPE ===
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
let flushIntervalMs = 2000;
let batchSize = 100;
let flushTimer: number | undefined;
let flushInProgress = false;
const unprovide: Array<() => void> = [];
const eventCleanups: Array<() => void> = [];

type EventSignal<T> = {
  subscribe: (cb: (arg: T) => void) => (arg: T) => void;
  unsubscribe: (cb: (arg: T) => void) => void;
};

function dimId(entityOrBlock?: Entity | Block | null): string {
  if (!entityOrBlock) return "";
  try {
    if ("isValid" in entityOrBlock && typeof (entityOrBlock as any).isValid === "function") {
      if (!(entityOrBlock as any).isValid()) return "";
    }
    return entityOrBlock.dimension?.id || "";
  } catch {
    return "";
  }
}

function safeLoc(target?: Entity | Block | Vector3 | null): [number | null, number | null, number | null] {
  if (!target) return [null, null, null];
  try {
    if ("x" in target && typeof target.x === "number") {
      return [Math.round(target.x), Math.round(target.y), Math.round(target.z)];
    }
    if ("isValid" in target && typeof (target as any).isValid === "function") {
      if (!(target as any).isValid()) return [null, null, null];
    }
    const loc = (target as Entity | Block).location;
    if (loc) {
      return [Math.round(loc.x), Math.round(loc.y), Math.round(loc.z)];
    }
  } catch {
    return [null, null, null];
  }
  return [null, null, null];
}

function safeTypeId(target?: Entity | Block | null): string {
  if (!target) return "";
  try {
    if ("isValid" in target && typeof (target as any).isValid === "function") {
      if (!(target as any).isValid()) return "";
    }
    return (target as any).typeId || "";
  } catch {
    return "";
  }
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
  if (flushInProgress || queue.length === 0) return;
  flushInProgress = true;
  const batch = takeBatch(queue, batchSize);
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
      err instanceof Error ? err : new Error(String(err))
    );
    queue = batch.concat(queue);
  } finally {
    flushInProgress = false;
    if (queue.length >= batchSize) void flush();
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
      input.payload && typeof input.payload === "object" ? (input.payload as Record<string, unknown>) : undefined,
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

  const where = clauses.length === 0 ? undefined : clauses.length === 1 ? clauses[0] : { and: clauses };

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
  try {
    if (!player) return;
    const isValid = (player as unknown as { isValid?: unknown }).isValid;
    if (typeof isValid === "function" && !isValid.call(player)) return;
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

function subscribeNative(): void {
  const AE = world.afterEvents;

  safeSubscribe(AE.playerSpawn, (event: PlayerSpawnAfterEvent) => {
    try {
      if (event.initialSpawn) playerEnqueue(event.player, "player.join");
      else playerEnqueue(event.player, "player.spawn");
    } catch (err) {
      debug.w("ActivityLog", "playerSpawn handler failed", err);
    }
  });

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

  safeSubscribe(AE.playerDimensionChange, (event: PlayerDimensionChangeAfterEvent) => {
    try {
      const [x, y, z] = safeLoc(event.toLocation);
      playerEnqueue(event.player, "player.dimension", {
        x,
        y,
        z,
        payload: {
          from: event.fromDimension?.id || "",
          to: event.toDimension?.id || "",
        },
      });
    } catch (err) {
      debug.w("ActivityLog", "playerDimensionChange handler failed", err);
    }
  });

  safeSubscribe(AE.playerGameModeChange, (event: PlayerGameModeChangeAfterEvent) => {
    try {
      playerEnqueue(event.player, "player.gamemode", {
        payload: { from: event.fromGameMode, to: event.toGameMode },
      });
    } catch (err) {
      debug.w("ActivityLog", "playerGameModeChange handler failed", err);
    }
  });

  safeSubscribe(AE.chatSend, (event: ChatSendAfterEvent) => {
    try {
      playerEnqueue(event.sender, "player.chat", {
        payload: { message: event.message },
      });
    } catch (err) {
      debug.w("ActivityLog", "chatSend handler failed", err);
    }
  });

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

  safeSubscribe(AE.entityDie, (event: EntityDieAfterEvent) => {
    try {
      const dead = event.deadEntity;
      const [x, y, z] = safeLoc(dead);
      const killer = event.damageSource?.damagingEntity;
      const killerTypeId = safeTypeId(killer);
      const deadTypeId = safeTypeId(dead);
      if (killerTypeId === "minecraft:player" && killer) {
        playerEnqueue(killer as Player, "entity.death", {
          targetId: deadTypeId === "minecraft:player" ? (dead as Player).id : deadTypeId,
          x,
          y,
          z,
          payload: { cause: event.damageSource?.cause },
        });
      } else if (deadTypeId === "minecraft:player") {
        playerEnqueue(dead as Player, "entity.death", {
          targetId: killerTypeId || event.damageSource?.cause || "unknown",
          x,
          y,
          z,
          payload: { cause: event.damageSource?.cause },
        });
      }
    } catch (err) {
      debug.w("ActivityLog", "entityDie handler failed", err);
    }
  });

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

  safeSubscribe(AE.entityHurt, (event: EntityHurtAfterEvent) => {
    try {
      if (safeTypeId(event.hurtEntity) !== "minecraft:player") return;
      playerEnqueue(event.hurtEntity as Player, "entity.hurt", {
        payload: {
          damage: event.damage,
          cause: event.damageSource?.cause,
        },
      });
    } catch (err) {
      debug.w("ActivityLog", "entityHurt handler failed", err);
    }
  });

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

  safeSubscribe(AE.entityItemDrop, (event: EntityItemDropAfterEvent) => {
    try {
      const e = event.entity;
      const items = (event.items || []).map((item: Entity) => safeTypeId(item)).filter(Boolean);
      if (safeTypeId(e) === "minecraft:player") {
        playerEnqueue(e as Player, "item.drop", { payload: { items } });
      }
    } catch (err) {
      debug.w("ActivityLog", "entityItemDrop handler failed", err);
    }
  });

  safeSubscribe(AE.entityItemPickup, (event: EntityItemPickupAfterEvent) => {
    try {
      if (safeTypeId(event.entity) !== "minecraft:player") return;
      playerEnqueue(event.entity as Player, "item.pickup", {
        payload: { items: (event.items || []).map((item: ItemStack) => item?.type?.id || "").filter(Boolean) },
      });
    } catch (err) {
      debug.w("ActivityLog", "entityItemPickup handler failed", err);
    }
  });

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

  safeSubscribe(AE.explosion, (event: ExplosionAfterEvent) => {
    try {
      const source = event.source;
      const sourceTypeId = safeTypeId(source);
      if (!source || sourceTypeId !== "minecraft:player") return;
      const [x, y, z] = safeLoc(source);
      const impacted = event.getImpactedBlocks ? event.getImpactedBlocks() : [];
      playerEnqueue(source as Player, "world.explosion", {
        x,
        y,
        z,
        level: "warn",
        payload: { impactedBlocks: impacted ? impacted.length : 0 },
      });
    } catch (err) {
      debug.w("ActivityLog", "explosion handler failed", err);
    }
  });
}
// === REGISTRY ===
ModuleRegistry.register({
  id: MODULE_ID,
  afterWorldLoad: false,
  lifecycle: {
    registerPermissions() {
      // 无命令面
    },
    registerEvents() {
      subscribeNative();
    },
    async init() {
      const flushMs = await config.get<number>("flush_interval_ms");
      const batch = await config.get<number>("batch_size");
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
      void flush();
      debug.i("ActivityLog", "cleanup");
    },
  },
});
