/**
 * CLI entrypoint for `load-gen` Playwright synthetic E2EE bots.
 */

import { runLoadBots } from "./botRunner";
import { parseCliArgs, printUsage } from "./cli";
import { ensureCpuPinned } from "./cpuPinning";

async function main(): Promise<void> {
  const config = parseCliArgs(process.argv.slice(2));
  if (config.helpRequested) {
    printUsage();
    return;
  }

  ensureCpuPinned(config.cpuCores);

  console.log(
    `[load-gen] Starting ${config.bots} bot(s) -> ${config.sfuUrl} (headless=${config.headless}, e2ee=${config.enableE2ee}, duration=${config.durationSec}s, cpuCores=${config.cpuCores ?? "unpinned"})`,
  );

  const summaries = await runLoadBots(config);
  console.log(JSON.stringify({ roomId: config.roomId, summaries }, null, 2));

  const failed = summaries.filter((item) => item.error !== undefined);
  if (failed.length > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("[load-gen] Fatal error:", err);
  process.exit(1);
});
