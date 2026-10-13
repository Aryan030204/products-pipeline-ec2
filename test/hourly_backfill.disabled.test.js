// hourly_backfill.js run() with HOURLY_BACKFILL_MODE unset.
import { test, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { setTestEnv, silenceConsole } from "./helpers.js";

setTestEnv({ TOTAL_CONFIG_COUNT: "0" });
delete process.env.HOURLY_BACKFILL_MODE;
const h = await import("../hourly_backfill.js");

afterEach(() => mock.restoreAll());

test("run exits early when HOURLY_BACKFILL_MODE is not true", async () => {
  silenceConsole();
  assert.equal(await h.run(), undefined);
  assert.match(console.log.mock.calls[0].arguments[0], /HOURLY_BACKFILL_MODE is not true/);
});
