// backfill_product_type_funnel.js run() with no date range and no lookback.
import { test, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import { setTestEnv, BRANDS_API, apiBrand, mockBrandsApi, silenceConsole } from "./helpers.js";

setTestEnv({ GET_BRANDS_API: BRANDS_API, PIPELINE_AUTH_HEADER: "hdr", PASSWORD_AES_KEY: "k" });
for (const key of [
  "BACKFILL_START_IST_DATE", "BACKFILL_START_IST", "BACKFILL_END_IST_DATE",
  "BACKFILL_END_IST", "BACKFILL_LOOKBACK_DAYS", "BACKFILL_BRAND_INDICES",
]) {
  delete process.env[key];
}
const f = await import("../backfill_product_type_funnel.js");

afterEach(() => mock.restoreAll());

test("run throws when neither a range nor a lookback window is configured", async () => {
  silenceConsole();
  mockBrandsApi(axios, mysql, { 1: apiBrand(1) });
  await assert.rejects(f.run(), /Requires BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE/);
});
