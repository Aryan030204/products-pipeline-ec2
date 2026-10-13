// BACKFILL_MODE=true with neither a START/END range nor a lookback window.
import { test, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import { setTestEnv, BRANDS_API, apiBrand, mockBrandsApi, silenceConsole } from "./helpers.js";

setTestEnv({
  GET_BRANDS_API: BRANDS_API,
  PIPELINE_AUTH_HEADER: "hdr",
  PASSWORD_AES_KEY: "k",
  BACKFILL_MODE: "true",
});
for (const key of [
  "BACKFILL_START_IST_DATE", "BACKFILL_START_IST", "BACKFILL_END_IST_DATE",
  "BACKFILL_END_IST", "BACKFILL_LOOKBACK_DAYS", "BACKFILL_BRAND_INDICES",
]) {
  delete process.env[key];
}
const p = await import("../pipeline.js");

afterEach(() => mock.restoreAll());

test("runBackfillPipeline refuses to run without a date range or lookback", async () => {
  silenceConsole();
  mockBrandsApi(axios, mysql, { 1: apiBrand(1) });
  await assert.rejects(p.runBackfillPipeline(), /requires BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE/);
});
