// backfill_product_type_funnel.js: processBrandDate + run() over an explicit range.
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
  makeBrand,
  makeFakePool,
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
  BACKFILL_START_IST_DATE: "2024-06-01",
  BACKFILL_END_IST_DATE: "2024-06-02",
  BACKFILL_BRAND_CONCURRENCY: "2",
  SHOPIFYQL_FETCH_MAX_RETRIES: "1",
});
delete process.env.BACKFILL_LOOKBACK_DAYS;
delete process.env.BACKFILL_BRAND_INDICES;
const f = await import("../backfill_product_type_funnel.js");

const DAY = "2024-06-01";

beforeEach(() => {
  silenceConsole();
  fastTimers();
});
afterEach(() => mock.restoreAll());

describe("processBrandDate", () => {
  test("ensures tables, then fetches, upserts and rolls up sales for the date", async () => {
    mockShopifyQL(axios);
    const brand = makeBrand();
    await f.processBrandDate(brand, DAY);
    const log = sqlLog(brand.pool);
    const order = [
      /CREATE TABLE IF NOT EXISTS product_sessions_snapshot/,
      /CREATE TABLE IF NOT EXISTS product_type_sales_snapshot/,
      /INSERT INTO product_type_sales_snapshot/,
      /UPDATE mv_product_type_funnel_daily/,
    ].map((re) => log.findIndex((s) => re.test(s)));
    assert.ok(order.every((i) => i >= 0), JSON.stringify(order));
    assert.deepEqual([...order].sort((a, b) => a - b), order);
    assert.ok(!log.some((s) => /INSERT INTO product_sessions_snapshot/.test(s)), "session data untouched");
  });

  test("logs and skips the date when the ShopifyQL fetch fails", async () => {
    mock.method(axios, "post", async () => httpResp(500));
    const brand = makeBrand();
    await f.processBrandDate(brand, DAY);
    assert.ok(!sqlLog(brand.pool).some((s) => /INSERT INTO (product_type_sales_snapshot|mv_product_type_funnel_daily)/.test(s)));
    assert.match(console.error.mock.calls[0].arguments[0], /Skipping product-type funnel backfill for 2024-06-01/);
  });

  test("propagates non-ShopifyQL errors", async () => {
    mockShopifyQL(axios);
    const brand = makeBrand({
      pool: makeFakePool((sql) => {
        if (/INSERT INTO product_type_sales_snapshot/.test(sql)) throw new Error("db gone");
        return defaultQueryHandler(sql);
      }),
    });
    await assert.rejects(f.processBrandDate(brand, DAY), /db gone/);
  });
});

describe("run (explicit range)", () => {
  test("backfills every date in the range for every brand", async () => {
    mockBrandsApi(axios, mysql, { 1: apiBrand(1), 2: apiBrand(2) });
    mockShopifyQL(axios);
    await f.run();

    const pools = mysql.createPool.mock.calls.map((c) => c.result);
    assert.equal(pools.length, 2);
    for (const pool of pools) {
      const dates = pool.calls
        .filter((c) => /INSERT INTO product_type_sales_snapshot/.test(c.sql))
        .map((c) => c.params[0]);
      assert.deepEqual(dates, ["2024-06-01", "2024-06-02"]);
    }
  });
});
