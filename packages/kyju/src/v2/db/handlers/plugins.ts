import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import { nanoid } from "nanoid";
import type { ServerEvent } from "../../shared";
import type { ClientProxy } from "../../client/client";
import type { SchemaShape } from "../schema";
import type { Session } from "../helpers";
import { createReplica } from "../../replica/replica";
import { createClient } from "../../client/client";
import type { DbHandlerContext } from "../helpers";
import { traceKyju } from "../../trace";

export type PluginContext = {
  client: ClientProxy<SchemaShape>;
  pluginPath: string[];
};

export type DbPlugin = {
  name: string;
  onBeforeStart?: (ctx: PluginContext) => Promise<void>;
};

export const runPlugins = (
  ctx: DbHandlerContext,
  postMessageEffect: (event: ServerEvent) => Effect.Effect<void, never, never>,
  plugins: DbPlugin[],
) =>
  Effect.gen(function* () {
    const sessionId = nanoid();
    const root = yield* ctx.rootCache.read();

    const replica = createReplica({
      send: (event) => {
        Effect.runPromise(postMessageEffect(event));
      },
      maxPageSizeBytes: ctx.config.maxPageSize,
    });

    const session: Session = {
      sessionId,
      replicaId: replica.replicaId,
      subscriptions: new Set(),
      send: (event) => {
        replica.postMessage(event);
      },
    };

    yield* Ref.update(ctx.sessionsRef, (s) => {
      const next = new Map(s);
      next.set(sessionId, session);
      return next;
    });

    replica._forceState({
      kind: "connected" as const,
      sessionId,
      root,
      rootVersion: 0,
      collections: [],
      blobs: [],
    });

    const client = createClient<SchemaShape>(replica);

    for (const plugin of plugins) {
      if (!plugin.onBeforeStart) continue;
      yield* traceKyju(
        "kyju:db.plugin.onBeforeStart",
        Effect.promise(() =>
          plugin.onBeforeStart!({
            client,
            pluginPath: ["_plugins", plugin.name],
          }),
        ),
        { plugin: plugin.name },
      );
    }

    return replica;
  });
