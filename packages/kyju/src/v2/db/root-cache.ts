import * as Effect from "effect/Effect";
import type { PlatformError } from "@effect/platform/Error";
import * as FileSystem from "@effect/platform/FileSystem";
import { paths, readJsonFile, writeJsonFile, type DbConfig } from "./helpers";
import type { KyjuJSON } from "../shared";
import { traceKyju } from "../trace";

/**
 * Authoritative in-memory copy of the root document, with coalesced disk
 * persistence.
 *
 * The root document is a single JSON file holding all top-level state. Two
 * properties of that design force this cache:
 *
 *  1. Reads must be O(1). Hot paths (write handlers, ref-checks, replica
 *     connect) need the current root cheaply; reading and parsing a JSON
 *     file per access scales poorly with both root size and access rate.
 *
 *  2. Writes amplify file IO: the whole document is rewritten on every
 *     mutation. With many small mutations arriving close together, doing a
 *     full read-modify-write per mutation produces work proportional to
 *     N × file_size, even when the actual change is tiny. Serializing those
 *     rewrites through a mutex (necessary for atomicity) further multiplies
 *     wall time as later writers wait for earlier ones to drain.
 *
 * The cache addresses both: in-process readers see an always-up-to-date
 * object reference, and writers update memory immediately while a single
 * deferred flush coalesces an arbitrary number of in-tick mutations into
 * one disk write.
 *
 * Contract:
 *   • `read()` returns the current state — including any mutations that
 *     completed earlier in the same fiber.
 *   • `set()` updates memory atomically (callers hold rootMutex around
 *     read+set) and schedules a flush; it does not await disk.
 *   • `flush()` blocks until the cache's current state is on disk; safe
 *     to call any time, idempotent when nothing is pending.
 *   • Disk writes acquire the same rootMutex as logical writes, so on-disk
 *     state can never reflect a torn intermediate object.
 *
 * Durability tradeoff: a write that has returned but not been flushed
 * survives an in-process failure (still in memory) but not a process death
 * (gone with the heap). Callers that need disk-on-return must call
 * `flush()` explicitly. Shutdown paths should always flush.
 */

export type RootCache = {
  /** Current in-memory root. Cheap; reflects all completed writes. */
  read: () => Effect.Effect<KyjuJSON>;
  /** Current root plus version metadata for incremental sync. */
  readRootInfo: () => Effect.Effect<{
    root: KyjuJSON;
    rootVersion: number;
    keyVersions: Record<string, number>;
    deletedKeyVersions: Record<string, number>;
  }>;
  /**
   * Replace the root in memory and schedule a coalesced disk flush.
   * `changedKeys` names the top-level keys that were actually modified
   * (path[0] from the write op). When omitted the cache diffs the full
   * root to detect changes — prefer passing explicit keys for accuracy
   * with key deletion tracking.
   */
  set: (root: KyjuJSON, changedKeys?: string[]) => Effect.Effect<void>;
  /** Block until disk reflects the current in-memory root. Idempotent. */
  flush: () => Effect.Effect<void>;
};

export const makeRootCache = (
  fs: FileSystem.FileSystem,
  config: DbConfig,
  rootMutex: Effect.Semaphore,
): Effect.Effect<RootCache, PlatformError> =>
  Effect.gen(function* () {
    const initial = yield* readJsonFile({ fs, path: paths.root({ config }) });
    let cached: KyjuJSON = initial;

    // Version counter for incremental reconnect sync. Every `set()`
    // increments the version and records which top-level keys changed.
    let rootVersion = 0;
    const keyVersions = new Map<string, number>();
    const deletedKeyVersions = new Map<string, number>();

    // Producer/consumer state for the coalescing flusher.
    //   • `pending`         — cache has unflushed changes
    //   • `scheduledTimer`  — a setImmediate has been queued to start a runner
    //   • `runner`          — an async loop is actively draining flushes
    // At most one of {scheduledTimer, runner} is non-null at a time.
    let scheduledTimer: NodeJS.Immediate | null = null;
    let runner: Promise<void> | null = null;
    let pending = false;

    // The disk write acquires rootMutex so it can never serialize a torn
    // intermediate state mid-mutation. The Effect captures `cached` by
    // reference; any further mutation that lands while this write is in
    // flight will set `pending=true` and be picked up by the runner's next
    // loop iteration.
    //
    // Disk failures are logged but not rethrown: the in-memory state remains
    // authoritative within this process; the next successful flush will
    // bring disk into sync. Throwing here would propagate into unrelated
    // code paths (other writers awaiting their own ops) for an issue they
    // cannot resolve.
    const doFlush = (): Promise<void> =>
      Effect.runPromise(
        rootMutex.withPermits(1)(
          traceKyju(
            "kyju:db.root.flush",
            writeJsonFile({ fs, config, path: paths.root({ config }), data: cached }),
          ),
        ).pipe(
          Effect.catchAll((err) => {
            // eslint-disable-next-line no-console
            console.error("[kyju:rootCache] flush failed:", err);
            return Effect.void;
          }),
        ),
      );

    const startRunner = (): void => {
      scheduledTimer = null;
      if (runner) return;
      runner = (async () => {
        while (pending) {
          pending = false;
          await doFlush();
        }
        runner = null;
      })();
    };

    const computeChangedKeys = (next: KyjuJSON): string[] => {
      const changed: string[] = [];
      if (
        typeof cached !== "object" || cached === null ||
        typeof next !== "object" || next === null ||
        Array.isArray(cached) !== Array.isArray(next)
      ) {
        // Type change — include every key from both old and new so the
        // delta sync doesn't silently drop an entire state tree.
        const all = new Set<string>();
        if (typeof cached === "object" && cached !== null && !Array.isArray(cached)) {
          for (const k of Object.keys(cached as Record<string, KyjuJSON>)) all.add(k);
        }
        if (typeof next === "object" && next !== null && !Array.isArray(next)) {
          for (const k of Object.keys(next as Record<string, KyjuJSON>)) all.add(k);
        }
        return Array.from(all);
      }
      const oldObj = cached as Record<string, KyjuJSON>;
      const newObj = next as Record<string, KyjuJSON>;
      const allKeys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);
      for (const key of allKeys) {
        if (key in oldObj !== key in newObj || oldObj[key] !== newObj[key]) {
          changed.push(key);
        }
      }
      return changed;
    };

    return {
      read: () => Effect.sync(() => cached),
      readRootInfo: () =>
        Effect.sync(() => ({
          root: cached,
          rootVersion,
          keyVersions: Object.fromEntries(keyVersions),
          deletedKeyVersions: Object.fromEntries(deletedKeyVersions),
        })),
      set: (root: KyjuJSON, changedKeys?: string[]) =>
        Effect.sync(() => {
          const keys = changedKeys ?? computeChangedKeys(root);

          if (changedKeys === undefined) {
            // Full-root diff: detect both additions/modifications and deletions.
            const resolvedDeleted: string[] = [];
            const resolvedChanged: string[] = [];
            for (const key of keys) {
              if (
                typeof cached === "object" && cached !== null &&
                !Array.isArray(cached) &&
                !(key in (cached as Record<string, KyjuJSON>))
              ) {
                resolvedChanged.push(key);
              } else if (
                typeof root === "object" && root !== null &&
                !Array.isArray(root) &&
                !(key in (root as Record<string, KyjuJSON>))
              ) {
                resolvedDeleted.push(key);
              } else {
                resolvedChanged.push(key);
              }
            }
            rootVersion++;
            for (const key of resolvedChanged) keyVersions.set(key, rootVersion);
            for (const key of resolvedDeleted) deletedKeyVersions.set(key, rootVersion);
          } else {
            rootVersion++;
            const resolvedDeleted: string[] = [];
            const resolvedChanged: string[] = [];
            for (const key of keys) {
              if (
                typeof root === "object" && root !== null &&
                !Array.isArray(root) &&
                !(key in (root as Record<string, KyjuJSON>))
              ) {
                resolvedDeleted.push(key);
              } else {
                resolvedChanged.push(key);
              }
            }
            for (const key of resolvedChanged) keyVersions.set(key, rootVersion);
            for (const key of resolvedDeleted) deletedKeyVersions.set(key, rootVersion);
          }

          cached = root;
          pending = true;
          if (runner || scheduledTimer) return;
          scheduledTimer = setImmediate(startRunner);
        }),
      flush: () =>
        Effect.gen(function* () {
          if (scheduledTimer) {
            const t = scheduledTimer;
            scheduledTimer = null;
            yield* Effect.sync(() => clearImmediate(t));
            startRunner();
          } else if (pending && !runner) {
            startRunner();
          }
          if (runner) {
            yield* Effect.promise(() => runner!);
          }
        }),
    };
  });
