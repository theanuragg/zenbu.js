import type { RouterProxy } from "./types";
import { serialize } from "./protocol";
import type { createPendingRequests } from "./pending";

type ProxyDeps = {
  send: (data: string) => void;
  pending: ReturnType<typeof createPendingRequests>;
};

// Property names that must not return a child proxy. Returning a truthy
// value for "then" makes JS treat the proxy as a thenable, causing
// `await rpc.app` to fire a bogus RPC call instead of erroring cleanly.
const RESERVED_PROPS = new Set(["then", "catch", "finally"]);

// Null byte is used as path segment separator. It cannot appear in a
// JavaScript property name, so segment boundaries are always unambiguous —
// unlike "." which would silently mis-route any service/method whose name
// contains a dot.
const SEP = "\x00";

export const createProxy = <T extends Record<string, any>>(
  deps: ProxyDeps,
): RouterProxy<T> => {
  // Counter scoped to each client instance so HMR module reloads don't reset
  // it and risk reusing IDs that are still in-flight on the old connection.
  let _nextId = 0;

  // Cache proxy objects by path so repeated accesses (e.g. `rpc.app` inside a
  // render loop) return the same reference without any allocation.
  const cache = new Map<string, unknown>();

  const makeProxy = (path: string): unknown => {
    const cached = cache.get(path);
    if (cached !== undefined) return cached;

    const proxy = new Proxy(function () {}, {
      get(_target, prop, _receiver) {
        if (typeof prop === "symbol") return undefined;
        // Prevent accidental thenable behavior when the proxy is awaited.
        if (RESERVED_PROPS.has(prop as string)) return undefined;
        const child = path ? `${path}${SEP}${prop as string}` : (prop as string);
        return makeProxy(child);
      },
      apply(_target, _thisArg, args: unknown[]) {
        const id = String(++_nextId);
        const promise = deps.pending.add(id);
        deps.send(
          serialize({
            type: "request",
            id,
            // Root path is "" — splitting it would produce [""] not [].
            path: path ? path.split(SEP) : [],
            args,
          }),
        );
        return promise;
      },
    });

    cache.set(path, proxy);
    return proxy;
  };

  return makeProxy("") as RouterProxy<T>;
};
