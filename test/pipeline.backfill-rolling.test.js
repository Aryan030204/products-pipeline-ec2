// BACKFILL_MODE with BACKFILL_LOOKBACK_DAYS (the Docker cron worker's mode) and
// BACKFILL_BRAND_INDICES filtering.
import { test, describe, before, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import {
  setTestEnv,
  BRANDS_API,
  apiBrand,
  mockBrandsApi,
  mockShopifyQL,
  httpResp,
  fastTimers,
  silenceConsole,
} from "./helpers.js";

setTestEnv({
  GET_BRANDS_API: BRANDS_API,
  PIPELINE_AUTH_HEADER: "hdr",
  PASSWORD_AES_KEY: "k",
  BACKFILL_MODE: "true",
  BACKFILL_LOOKBACK_DAYS: "2",
  BACKFILL_BRAND_INDICES: "0,2,9",
  BACKFILL_BRAND_CONCURRENCY: "3",
});
const p = await import("../pipeline.js");

let brands;
before(async () => {
  silenceConsole();
  mockBrandsApi(axios, mysql, {
    1: apiBrand(1),
    2: apiBrand(2),
    3: apiBrand(3, { store_timezone: "America/Los_Angeles" }),
  });
  brands = await p.getBrands();
  mock.restoreAll();
});

beforeEach(() => silenceConsole());
afterEach(() => {
  mock.restoreAll();
  mock.timers.reset();
});

describe("selectBackfillBrands with BACKFILL_BRAND_INDICES=0,2,9", () => {
  test("keeps only the requested indices and warns about unknown ones", () => {
    const selected = p.selectBackfillBrands(brands);
    assert.deepEqual(selected.map((b) => b.index), [0, 2]);
    assert.match(console.warn.mock.calls[0].arguments[0], /no active match: 9/);
  });

  test("throws when no requested index matches", () => {
    assert.throws(
      () => p.selectBackfillBrands([{ index: 5, name: "x" }]),
      /matched no active brands. Active brand indices: 5/,
    );
  });
});

describe("runBackfillPipeline (rolling)", () => {
  test("refetches each selected brand's own last N local days", async () => {
    fastTimers();
    // 2025-03-01T03:00Z: Mar 1 in Kolkata, still Feb 28 in Los Angeles.
    mock.timers.enable({ apis: ["Date"], now: Date.parse("2025-03-01T03:00:00Z") });
    for (const b of brands) b.pool.calls = [];
    mock.method(axios, "get", async () => httpResp(200, { products: [] }));
    mockShopifyQL(axios);

    await p.runBackfillPipeline();

    const dates = (b) =>
      b.pool.calls.filter((c) => /INSERT INTO product_sessions_snapshot/.test(c.sql)).map((c) => c.params[0]);
    assert.deepEqual(dates(brands[0]), ["2025-02-28", "2025-03-01"]);
    assert.deepEqual(dates(brands[1]), [], "index 1 not selected");
    assert.deepEqual(dates(brands[2]), ["2025-02-27", "2025-02-28"]);
  });
});
