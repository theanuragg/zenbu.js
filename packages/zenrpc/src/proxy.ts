import type { RouterProxy } from "./types";
import { serialize } from "./protocol";
import type { createPendingRequests } from "./pending";

type ProxyDeps = {
  send: (data: string) => void;
  pending: ReturnType<typeof createPendingRequests>;
};

// Monotonic counter for request IDs. IDs only need to be unique within a
// single WebSocket session — a simple integer is correct and far cheaper
// than nanoid's crypto.getRandomValues call.
let _nextId = 0;
const nextId = (): string => String(++_nextId);

export const createProxy = <T extends Record<string, any>>(
  deps: ProxyDeps,
): RouterProxy<T> => {
  // Cache proxy objects by path string so repeated accesses (e.g. `rpc.app`
  // inside a render loop) return the same reference without any allocation.
  const cache = new Map<string, unknown>();

  const makeProxy = (path: string): unknown => {
    let proxy = cache.get(path);
    if (proxy !== undefined) return proxy;

    proxy = new Proxy(function () {}, {
      get(_target, prop, _receiver) {
        if (typeof prop === "symbol") return undefined;
        const child = path ? `${path}.${prop as string}` : (prop as string);
        return makeProxy(child);
      },
      apply(_target, _thisArg, args: unknown[]) {
        const id = nextId();
        const promise = deps.pending.add(id);
        deps.send(
          serialize({
            type: "request",
            id,
            // Guard: root path is "" — split would produce [""] not [].
            path: path ? path.split(".") : [],
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
