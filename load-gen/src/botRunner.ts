/**
 * Playwright Chromium synthetic load & E2EE SFrame benchmark runner.
 *
 * Enforces CLAUDE.md host hardware rules:
 * - Defaults to `headless: true` with low-overhead Chromium flags
 *   (`--use-fake-ui-for-media-stream`, `--use-fake-device-for-media-stream`,
 *   `--disable-gpu`, `--disable-dev-shm-usage`).
 * - Always tears down all `BrowserContext` and `Browser` instances in `finally`
 *   and upon `SIGINT`/`SIGTERM` so 8 GB RAM is never leaked to zombie Chrome tabs.
 */

import { randomBytes } from "crypto";
import { chromium, type Browser, type BrowserContext } from "playwright";
import type { LoadGenCliConfig } from "./cli";
import {
  sframeBrowserInitScript,
  type BrowserSframeStats,
  type SframeInitOptions,
} from "./sframeInitScript";

interface ClientPageMetrics {
  roomId: string;
  peerIdHint: string;
  startRequestedAtMs: number | null;
  firstRemoteFrameAtMs: number | null;
  ttffMs: number | null;
  consumedKinds: string[];
  failoverEvents: unknown[];
}

export interface BotRunSummary {
  botId: string;
  ttffMs: number | null;
  consumedKinds: string[];
  sframe: BrowserSframeStats | null;
  failoverEventsReceived: number;
  error?: string;
}

export async function runLoadBots(
  config: LoadGenCliConfig,
): Promise<BotRunSummary[]> {
  // Local client-side room secret generated solely inside load-gen for Playwright
  // contexts; never transmitted to sfu-node, Socket.IO, or Kafka.
  const localClientRoomKeyHex = randomBytes(16).toString("hex");
  const initOptions: SframeInitOptions = {
    enabled: config.enableE2ee,
    keyHex: localClientRoomKeyHex,
    keyId: 1,
  };

  let browser: Browser | null = null;
  const contexts: BrowserContext[] = [];
  let shuttingDown = false;

  const cleanup = async (): Promise<void> => {
    for (const context of contexts) {
      await context.close().catch(() => {});
    }
    contexts.length = 0;

    if (browser) {
      await browser.close().catch(() => {});
      browser = null;
    }
  };

  const handleSignal = (signal: string): void => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    console.warn(`[load-gen] Received ${signal}; closing Chromium instances...`);
    void cleanup().finally(() => process.exit(0));
  };

  process.once("SIGINT", () => handleSignal("SIGINT"));
  process.once("SIGTERM", () => handleSignal("SIGTERM"));

  try {
    browser = await chromium.launch({
      headless: config.headless,
      args: [
        "--use-fake-ui-for-media-stream",
        "--use-fake-device-for-media-stream",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--autoplay-policy=no-user-gesture-required",
      ],
    });

    const botTasks: Array<Promise<BotRunSummary>> = [];
    for (let index = 1; index <= config.bots; index += 1) {
      const botId = `bot-${index}`;
      botTasks.push(
        runSingleBot(browser, contexts, config, initOptions, botId),
      );
    }

    return await Promise.all(botTasks);
  } finally {
    await cleanup();
  }
}

async function runSingleBot(
  browser: Browser,
  contexts: BrowserContext[],
  config: LoadGenCliConfig,
  initOptions: SframeInitOptions,
  botId: string,
): Promise<BotRunSummary> {
  const context = await browser.newContext({
    permissions: ["camera", "microphone"],
  });
  contexts.push(context);

  await context.addInitScript(sframeBrowserInitScript, initOptions);
  const page = await context.newPage();

  const targetUrl = new URL(config.sfuUrl);
  targetUrl.searchParams.set("roomId", config.roomId);
  targetUrl.searchParams.set("peerId", botId);

  try {
    await page.goto(targetUrl.toString(), { waitUntil: "domcontentloaded" });
    await page.waitForFunction(
      () =>
        Boolean(
          (window as unknown as { __sfugueClientReady?: boolean }).__sfugueClientReady,
        ),
      undefined,
      { timeout: 15_000 },
    );
    await page.click("#startBtn");

    await page.waitForFunction(
      () => {
        const video = document.getElementById("remoteVideo") as HTMLVideoElement | null;
        return Boolean(video && video.readyState >= 2 && video.srcObject !== null);
      },
      undefined,
      { timeout: 15_000 },
    );

    await page.waitForTimeout(config.durationSec * 1000);

    const extracted = await page.evaluate(() => {
      const w = window as unknown as {
        __sfugueMetrics?: ClientPageMetrics;
        __sfugueSframeStats?: BrowserSframeStats;
      };
      return {
        metrics: w.__sfugueMetrics ?? null,
        sframe: w.__sfugueSframeStats ?? null,
      };
    });

    return {
      botId,
      ttffMs: extracted.metrics?.ttffMs ?? null,
      consumedKinds: extracted.metrics?.consumedKinds ?? [],
      sframe: extracted.sframe
        ? {
            enabled: extracted.sframe.enabled,
            epoch: extracted.sframe.epoch,
            framesEncrypted: extracted.sframe.framesEncrypted,
            framesDecrypted: extracted.sframe.framesDecrypted,
            decryptionErrors: extracted.sframe.decryptionErrors,
            firstFrameDecryptedAtMs: extracted.sframe.firstFrameDecryptedAtMs,
            lastRatchetDurationMs: extracted.sframe.lastRatchetDurationMs,
          }
        : null,
      failoverEventsReceived: extracted.metrics?.failoverEvents.length ?? 0,
    };
  } catch (err) {
    return {
      botId,
      ttffMs: null,
      consumedKinds: [],
      sframe: null,
      failoverEventsReceived: 0,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
