/**
 * Fallback control-plane adapters implementing `IControlPlaneBroker`.
 *
 * - `RedisBroker`: Drop-in contingency adapter for empirical Kafka vs. Redis
 *   failover latency comparisons (CLAUDE.md Section 3 / Viva Contingency).
 *   Provides a minimal RESP2 TCP client over Node's built-in `net` module so
 *   zero external npm dependencies are required unless Redis mode is selected.
 * - `NoopControlPlaneBroker`: Allows standalone local SFU runs without Kafka.
 */

import * as net from "net";
import { assertNoForbiddenSecrets } from "./sanitizer";
import type { BrokerMessageHandler, IControlPlaneBroker } from "./types";

interface RedisEndpoint {
  host: string;
  port: number;
}

function parseRedisUrl(url: string): RedisEndpoint {
  try {
    const parsed = new URL(url);
    return {
      host: parsed.hostname || "127.0.0.1",
      port: parsed.port ? Number.parseInt(parsed.port, 10) : 6379,
    };
  } catch {
    return { host: "127.0.0.1", port: 6379 };
  }
}

function encodeRespCommand(args: string[]): string {
  const parts = [`*${args.length}\r\n`];
  for (const arg of args) {
    const byteLen = Buffer.byteLength(arg, "utf8");
    parts.push(`$${byteLen}\r\n${arg}\r\n`);
  }
  return parts.join("");
}

export class RedisBroker implements IControlPlaneBroker {
  public readonly kind = "redis" as const;

  private readonly endpoint: RedisEndpoint;
  private pubSocket: net.Socket | null = null;
  private readonly subSockets: net.Socket[] = [];
  private connected = false;

  constructor(redisUrl: string) {
    this.endpoint = parseRedisUrl(redisUrl);
  }

  public async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    this.pubSocket = await this.openSocket();
    this.connected = true;
  }

  public async publish<T extends object>(
    topic: string,
    message: T,
    _key?: string,
  ): Promise<void> {
    assertNoForbiddenSecrets(message);
    if (!this.pubSocket || !this.connected) {
      throw new Error("RedisBroker is not connected");
    }
    const payload = JSON.stringify(message);
    await new Promise<void>((resolve, reject) => {
      this.pubSocket?.write(encodeRespCommand(["PUBLISH", topic, payload]), (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  public async subscribe<T>(
    topic: string,
    _groupId: string,
    handler: BrokerMessageHandler<T>,
  ): Promise<void> {
    const socket = await this.openSocket();
    this.subSockets.push(socket);

    let buffer = "";
    socket.on("data", (chunk: Buffer) => {
      buffer += chunk.toString("utf8");
      const lines = buffer.split("\r\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (line.startsWith("{") && line.endsWith("}")) {
          try {
            const parsed = JSON.parse(line) as T;
            assertNoForbiddenSecrets(parsed);
            void Promise.resolve(handler(parsed)).catch((err: unknown) => {
              console.error(`[RedisBroker] Handler error on "${topic}":`, err);
            });
          } catch {
            // Ignore non-JSON RESP framing lines
          }
        }
      }
    });

    socket.write(encodeRespCommand(["SUBSCRIBE", topic]));
  }

  public async healthCheck(): Promise<boolean> {
    return this.connected && this.pubSocket !== null && !this.pubSocket.destroyed;
  }

  public async disconnect(): Promise<void> {
    for (const sock of this.subSockets) {
      sock.destroy();
    }
    this.subSockets.length = 0;

    if (this.pubSocket) {
      this.pubSocket.destroy();
      this.pubSocket = null;
    }
    this.connected = false;
  }

  private openSocket(): Promise<net.Socket> {
    return new Promise<net.Socket>((resolve, reject) => {
      const sock = net.createConnection({
        host: this.endpoint.host,
        port: this.endpoint.port,
      });
      sock.once("connect", () => resolve(sock));
      sock.once("error", (err) => reject(err));
    });
  }
}

export class NoopControlPlaneBroker implements IControlPlaneBroker {
  public readonly kind = "noop" as const;

  public async connect(): Promise<void> {}

  public async publish<T extends object>(
    _topic: string,
    message: T,
    _key?: string,
  ): Promise<void> {
    assertNoForbiddenSecrets(message);
  }

  public async subscribe<T>(
    _topic: string,
    _groupId: string,
    _handler: BrokerMessageHandler<T>,
  ): Promise<void> {}

  public async healthCheck(): Promise<boolean> {
    return true;
  }

  public async disconnect(): Promise<void> {}
}
