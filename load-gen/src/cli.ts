/**
 * CLI and environment argument parser for `load-gen`.
 *
 * Enforces hardware-conscious defaults for the 8 GB RAM / Intel i3 host
 * (`headless: true`, bounded bot count `1..8`, explicit CPU core pinning).
 */

export interface LoadGenCliConfig {
  sfuUrl: string;
  headless: boolean;
  cpuCores: string | null;
  bots: number;
  durationSec: number;
  enableE2ee: boolean;
  roomId: string;
  helpRequested: boolean;
}

const CPU_CORES_PATTERN = /^\d+(-\d+)?(,\d+(-\d+)?)*$/;

function parseBooleanEnv(raw: string | undefined, fallback: boolean): boolean {
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const normalized = raw.trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") {
    return true;
  }
  if (normalized === "false" || normalized === "0" || normalized === "no") {
    return false;
  }
  return fallback;
}

function parsePositiveInt(
  raw: string | undefined,
  flagName: string,
  fallback: number,
): number {
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Invalid value for ${flagName}: "${raw}" (expected >= 1)`);
  }
  return parsed;
}

export function printUsage(): void {
  console.log(`Usage: sfugue-load-gen [options]

Options:
  --sfu-url <url>        SFU signaling URL (default: http://127.0.0.1:3000, env: SFU_URL)
  --headless             Run Chromium headless (default: true, env: HEADLESS)
  --no-headless|--headed Run Chromium with visible UI for viva demos
  --cpu-cores <list>     Pin load-gen + Chromium via taskset -c (e.g. "2,3" or "0-1", env: BOT_CPU_CORES)
  --bots <n>             Number of concurrent bot contexts (1..8, default: 1, env: BOT_COUNT)
  --duration-sec <n>     Streaming duration per run in seconds (default: 30, env: DURATION_SEC)
  --e2ee / --no-e2ee     Enable/disable client-side SFrame AES-GCM transforms (default: true, env: ENABLE_E2EE)
  --room-id <id>         Target room ID (default: "default", env: ROOM_ID)
  --help                 Show this help message
`);
}

export function parseCliArgs(argv: string[]): LoadGenCliConfig {
  const config: LoadGenCliConfig = {
    sfuUrl: process.env.SFU_URL?.trim() || "http://127.0.0.1:3000",
    headless: parseBooleanEnv(process.env.HEADLESS, true),
    cpuCores: process.env.BOT_CPU_CORES?.trim() || null,
    bots: Math.min(8, parsePositiveInt(process.env.BOT_COUNT, "BOT_COUNT", 1)),
    durationSec: parsePositiveInt(process.env.DURATION_SEC, "DURATION_SEC", 30),
    enableE2ee: parseBooleanEnv(process.env.ENABLE_E2EE, true),
    roomId: process.env.ROOM_ID?.trim() || "default",
    helpRequested: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg) {
      continue;
    }

    switch (arg) {
      case "--help":
      case "-h":
        config.helpRequested = true;
        break;
      case "--headless":
        config.headless = true;
        break;
      case "--no-headless":
      case "--headed":
        config.headless = false;
        break;
      case "--e2ee":
        config.enableE2ee = true;
        break;
      case "--no-e2ee":
        config.enableE2ee = false;
        break;
      case "--sfu-url": {
        const value = argv[++i];
        if (!value) {
          throw new Error("--sfu-url requires a URL argument");
        }
        config.sfuUrl = value;
        break;
      }
      case "--cpu-cores": {
        const value = argv[++i];
        if (!value) {
          throw new Error("--cpu-cores requires a core list (e.g. 2,3)");
        }
        config.cpuCores = value.trim();
        break;
      }
      case "--bots": {
        const value = argv[++i];
        const parsed = parsePositiveInt(value, "--bots", 1);
        if (parsed > 8) {
          console.warn(
            `[cli] Clamping --bots=${parsed} to 8 to preserve 8 GB RAM host headroom`,
          );
        }
        config.bots = Math.min(8, parsed);
        break;
      }
      case "--duration-sec": {
        const value = argv[++i];
        config.durationSec = parsePositiveInt(value, "--duration-sec", 30);
        break;
      }
      case "--room-id": {
        const value = argv[++i];
        if (!value) {
          throw new Error("--room-id requires a room identifier");
        }
        config.roomId = value.trim();
        break;
      }
      default:
        throw new Error(`Unknown CLI argument: ${arg}`);
    }
  }

  if (config.cpuCores !== null && !CPU_CORES_PATTERN.test(config.cpuCores)) {
    throw new Error(
      `Invalid --cpu-cores "${config.cpuCores}". Expected format like "2,3" or "0-1".`,
    );
  }

  return config;
}
