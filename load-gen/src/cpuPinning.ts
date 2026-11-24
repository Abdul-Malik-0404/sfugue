/**
 * CPU affinity helper for `load-gen` (CLAUDE.md Section 5).
 *
 * Re-executes the process inside `taskset -c <coreList>` so Playwright
 * Chromium encoding and SFrame AES-GCM encryption stay pinned to dedicated
 * cores, preventing observer-effect CPU contention with KinD/Mediasoup pods.
 */

import { spawnSync } from "child_process";

export function ensureCpuPinned(cpuCores: string | null): void {
  if (!cpuCores || process.env.SFUGUE_PINNED === "1") {
    return;
  }

  if (process.platform !== "linux") {
    console.warn(
      `[cpuPinning] CPU pinning (--cpu-cores=${cpuCores}) is only supported on Linux; continuing unpinned.`,
    );
    return;
  }

  const result = spawnSync(
    "taskset",
    ["-c", cpuCores, process.execPath, ...process.execArgv, ...process.argv.slice(1)],
    {
      stdio: "inherit",
      env: {
        ...process.env,
        SFUGUE_PINNED: "1",
      },
    },
  );

  if (result.error) {
    const err = result.error as NodeJS.ErrnoException;
    if (err.code === "ENOENT") {
      console.warn(
        `[cpuPinning] "taskset" binary not found; continuing without CPU pinning.`,
      );
      return;
    }
    throw result.error;
  }

  process.exit(result.status ?? 0);
}
