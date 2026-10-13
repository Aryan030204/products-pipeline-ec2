// BACKFILL_MODE with an explicit START/END range (one-off historical backfill).
import { test, before, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import { todayYMDInTimeZone } from "../shopifyql_utils.js";
import {
  setTestEnv,
  BRANDS_API,
  apiBrand,
  mockBrandsApi,
  mockShopifyQL,
  defaultQueryHandler,
  httpResp,
  sqlLog,
  fastTimers,
  silenceConsole,
} from "./helpers.js";

setTestEnv({
  GET_BRANDS_API: BRANDS_API,
  PIPELINE_AUTH_HEADER: "hdr",
  PASSWORD_AES_KEY: "k",
  BACKFILL_MODE: "true",
  // The trailing time part is stripped by pipeline.js.
  BACKFILL_START_IST_DATE: "2024-06-01T00:00:00+05:30",
  BACKFILL_END_IST_DATE: "2024-06-03",
  BACKFILL_BRAND_CONCURRENCY: "1",
  HOURLY_PRODUCT_SESSIONS_ENABLED: "true",
});
delete process.env.HOURLY_PRODUCT_SESSIONS_BACKFILL_ENABLED;
delete process.env.BACKFILL_LOOKBACK_DAYS;
delete process.env.BACKFILL_BRAND_INDICES;
const p = await import("../pipeline.js");

// Stateful pipeline_metadata so "product sync once per real day" is observable.
let lastSync;
const handlers = {
  current: (sql, params) => {
    if (/SELECT key_value FROM pipeline_metadata/.test(sql)) {
      return [lastSync ? [{ key_value: lastSync }] : [], []];
    }
    if (/INSERT INTO pipeline_metadata/.test(sql)) lastSync = params[0];
    return defaultQueryHandler(sql);
  },
};

let brands;
before(async () => {
  silenceConsole();
  mockBrandsApi(axios, mysql, { 1: apiBrand(1), 2: apiBrand(2), 3: apiBrand(3) }, handlers);
  brands = await p.getBrands();
  mock.restoreAll();
});

beforeEach(() => {
  silenceConsole();
  fastTimers();
  lastSync = null;
  for (const b of brands) b.pool.calls = [];
  mock.method(axios, "get", async () => httpResp(200, { products: [] }));
  mockShopifyQL(axios);
});
afterEach(() => mock.restoreAll());

const snapshotDates = (pool) =>
  pool.calls.filter((c) => /INSERT INTO product_sessions_snapshot/.test(c.sql)).map((c) => c.params[0]);

test("runBackfillPipeline processes every date for every brand", async () => {
  await p.runBackfillPipeline();
  for (const b of brands) {
    assert.deepEqual(snapshotDates(b.pool), ["2024-06-01", "2024-06-02", "2024-06-03"]);
  }
});

test("hourly product sessions are skipped in backfill unless explicitly enabled", async () => {
  await p.runBackfillPipeline();
  for (const b of brands) {
    const log = sqlLog(b.pool);
    assert.ok(log.some((s) => /INSERT INTO hourly_sessions_summary_shopify/.test(s)));
    assert.ok(!log.some((s) => /INSERT INTO hourly_product_sessions/.test(s)));
  }
});

test("product sync runs once per real day, not once per backfilled date", async () => {
  await p.processBrand(brands[0], "2024-06-01");
  await p.processBrand(brands[0], "2024-06-02");
  assert.equal(axios.get.mock.callCount(), 1);
  assert.equal(lastSync, todayYMDInTimeZone("Asia/Kolkata"));
});

test("safeRun runs the backfill (not the brand-local-today run) in BACKFILL_MODE", async () => {
  await p.safeRun("backfill_startup");
  assert.deepEqual(snapshotDates(brands[0].pool), ["2024-06-01", "2024-06-02", "2024-06-03"]);
});
