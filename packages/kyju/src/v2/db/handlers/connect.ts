import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import { nanoid } from "nanoid";
import type { KyjuJSON, ServerEvent } from "../../shared";
import { VERSION } from "../../shared";
import type { Session } from "../helpers";
import { makeAck, makeErrorAck, sendAck } from "../helpers";
import type { DbHandlerContext } from "../helpers";

type ConnectEvent = Extract<ServerEvent, { kind: "connect" }>;

export const handleConnect = (
  ctx: DbHandlerContext,
  event: ConnectEvent,
  latch: Effect.Latch,
) =>
  latch.whenOpen(
    Effect.gen(function* () {
      const msg = event.message;
      const { replicaId } = msg;

      if (msg.version !== VERSION) {
        ctx.dbSend({
          kind: "db-update",
          replicaId,
          message: makeErrorAck({
            requestId: msg.requestId,
            _tag: "VersionMismatchError",
            message: `Expected version ${VERSION}, got ${msg.version}`,
          }),
        });
        return;
      }

      const sessionId = nanoid();
      const { root, rootVersion, keyVersions, deletedKeyVersions } =
        yield* ctx.rootCache.readRootInfo();

      const session: Session = {
        sessionId,
        replicaId,
        subscriptions: new Set(),
        send: (event) => ctx.dbSend({ ...event, replicaId }),
      };
      yield* Ref.update(ctx.sessionsRef, (sessions) => {
        const next = new Map(sessions);
        next.set(sessionId, session);
        return next;
      });

      const lastRootVersion = msg.lastRootVersion ?? -1;

      if (lastRootVersion === rootVersion) {
        // No changes since the client's last known version. Send a
        // lightweight ack — the client keeps its current root.
        sendAck({
          session,
          ack: makeAck({
            requestId: msg.requestId,
            sessionId,
            data: {
              root: null,
              rootVersion,
              changedKeys: [],
              removedKeys: [],
              isPartial: true,
            },
          }),
        });
        return;
      }

      if (lastRootVersion >= 0) {
        // Delta sync: only send top-level keys that changed since
        // `lastRootVersion`. The client merges these into its existing
        // in-memory root (preserving unchanged keys).
        const changedKeys: string[] = [];
        const removedKeys: string[] = [];

        for (const [key, version] of Object.entries(keyVersions)) {
          if (version > lastRootVersion) changedKeys.push(key);
        }
        for (const [key, version] of Object.entries(deletedKeyVersions)) {
          if (version > lastRootVersion) removedKeys.push(key);
        }

        const partialRoot: Record<string, KyjuJSON> = {};
        if (typeof root === "object" && root !== null && !Array.isArray(root)) {
          const rootObj = root as Record<string, KyjuJSON>;
          for (const key of changedKeys) {
            if (key in rootObj) {
              partialRoot[key] = rootObj[key];
            }
          }
        }

        sendAck({
          session,
          ack: makeAck({
            requestId: msg.requestId,
            sessionId,
            data: {
              root: partialRoot,
              rootVersion,
              changedKeys,
              removedKeys,
              isPartial: true,
            },
          }),
        });
        return;
      }

      // Full root: first-time connect or backward-compat path.
      sendAck({
        session,
        ack: makeAck({
          requestId: msg.requestId,
          sessionId,
          data: { root, rootVersion },
        }),
      });
    }),
  );
