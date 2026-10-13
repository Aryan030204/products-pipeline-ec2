// hourly_backfill.js (legacy standalone hourly-summary backfill).
// The module calls run() on import; TOTAL_CONFIG_COUNT=0 makes that import-time
// run a harmless no-op, and tests then configure brands before calling run().
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import {
  setTestEnv,
  makeBrand,
  makeFakePool,
  httpResp,
  shopifyqlResp,
  queueAxiosPost,
  fastTimers,
  silenceConsole,
} from "./helpers.js";

setTestEnv({
  HOURLY_BACKFILL_MODE: "true",
  HOURLY_BACKFILL_START_IST_DATE: "2024-06-01",
  HOURLY_BACKFILL_END_IST_DATE: "2024-06-02",
  SHOPIFYQL_TIMEZONE: "Europe/Berlin",
  TOTAL_CONFIG_COUNT: "0",
});
const h = await import("../hourly_backfill.js");

const DAY = "2024-06-01";
const BRAND_ENV_KEYS = [];

function setBrandEnv(i, vars) {
  for (const [k, v] of Object.entries(vars)) {
    const key = `${k}_${i}`;
    BRAND_ENV_KEYS.push(key);
    process.env[key] = v;
  }
}

function fullBrandEnv(i, overrides = {}) {
  setBrandEnv(i, {
    IS_ACTIVE: "true",
    SHOP_NAME: `shop${i}`,
    API_VERSION: "2025-10",
    ACCESS_TOKEN: `tok${i}`,
    DB_HOST: `db${i}.local`,
    DB_USER: "u",
    DB_PASSWORD: "pw",
    DB_DATABASE: `d${i}`,
    ...overrides,
  });
}

let pools;
beforeEach(() => {
  silenceConsole();
  pools = [];
  mock.method(mysql, "createPool", (config) => {
    const pool = makeFakePool();
    pool.config = config;
    pools.push(pool);
    return pool;
  });
});
afterEach(() => {
  mock.restoreAll();
  for (const key of BRAND_ENV_KEYS.splice(0)) delete process.env[key];
  delete process.env.DB_SSL_ENABLED;
  process.env.TOTAL_CONFIG_COUNT = "0";
});

describe("date helpers", () => {
  test("isValidYMD / ymdToUTCDate / fmtUTCDateToYMD / addDaysUTC", () => {
    assert.equal(h.isValidYMD("2024-01-02"), true);
    assert.equal(h.isValidYMD("2024-1-2"), false);
    const d = h.ymdToUTCDate("2024-12-31");
    assert.equal(d.toISOString(), "2024-12-31T00:00:00.000Z");
    assert.equal(h.fmtUTCDateToYMD(h.addDaysUTC(d, 1)), "2025-01-01");
  });

  test("buildInclusiveDateRangeYMD", () => {
    assert.deepEqual(h.buildInclusiveDateRangeYMD("2024-02-28", "2024-03-01"), [
      "2024-02-28",
      "2024-02-29",
      "2024-03-01",
    ]);
    assert.throws(() => h.buildInclusiveDateRangeYMD("x", "2024-01-01"), /Invalid date format/);
    assert.throws(() => h.buildInclusiveDateRangeYMD("2024-01-02", "2024-01-01"), /Start > end/);
  });
});

describe("loadBrands", () => {
  test("loads active brands from indexed env, skipping inactive and incomplete ones", () => {
    process.env.TOTAL_CONFIG_COUNT = "3";
    fullBrandEnv(0, { BRAND_TAG: "zero" });
    fullBrandEnv(1, { IS_ACTIVE: "false" });
    fullBrandEnv(2, { DB_DATABASE: "" });
    const brands = h.loadBrands();
    assert.equal(brands.length, 1);
    assert.equal(brands[0].index, 0);
    assert.equal(brands[0].tag, "zero");
    assert.equal(brands[0].name, "ZERO");
    assert.equal(brands[0].pool, pools[0]);
    assert.equal(pools[0].config.database, "d0");
    assert.equal(pools[0].config.ssl, undefined);
  });

  test("auto-enables TLS only for rds.amazonaws.com hosts, overridable per brand", () => {
    process.env.TOTAL_CONFIG_COUNT = "3";
    fullBrandEnv(0, { DB_HOST: "a.rds.amazonaws.com" });
    fullBrandEnv(1, { DB_HOST: "b.rds.amazonaws.com", DB_SSL_ENABLED: "false" });
    fullBrandEnv(2, { DB_SSL_CA: "line1\\nline2" });
    h.loadBrands();
    assert.deepEqual(pools[0].config.ssl, { rejectUnauthorized: false });
    assert.equal(pools[1].config.ssl, undefined);
    assert.deepEqual(pools[2].config.ssl, { rejectUnauthorized: false, ca: "line1\nline2" });
  });

  test("returns [] when TOTAL_CONFIG_COUNT is 0", () => {
    assert.deepEqual(h.loadBrands(), []);
  });
});

describe("ensureHourlyTable", () => {
  test("creates hourly_sessions_summary_shopify and releases", async () => {
    const brand = makeBrand();
    await h.ensureHourlyTable(brand);
    assert.match(brand.pool.calls[0].sql, /CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify/);
    assert.equal(brand.pool.released, 1);
  });
});

describe("buildShopifyQLHourlyQuery", () => {
  test("uses SHOPIFYQL_TIMEZONE and a full-day window", () => {
    const q = h.buildShopifyQLHourlyQuery(DAY);
    assert.ok(!q.includes("\n"));
    assert.match(q, /WITH TIMEZONE 'Europe\/Berlin'/);
    assert.match(q, /SINCE 2024-06-01T00:00:00 UNTIL 2024-06-01T23:59:59/);
    assert.match(q, /GROUP BY hour_of_day/);
  });
});

describe("formatShopifyQLTable", () => {
  test("handles object rows, array rows and junk", () => {
    assert.deepEqual(
      h.formatShopifyQLTable({
        columns: [{ name: "a" }, { name: "b" }],
        rows: [{ a: 1 }, [2, 3], null],
      }),
      [{ a: 1 }, { a: 2, b: 3 }],
    );
    assert.deepEqual(h.formatShopifyQLTable({}), []);
  });
});

describe("fetchShopifyQLHourlySessions", () => {
  test("posts to the (hard-coded 2025-10) GraphQL endpoint and returns rows", async () => {
    const post = queueAxiosPost(axios, [shopifyqlResp([{ hour_of_day: 0, sessions: 1 }])]);
    const rows = await h.fetchShopifyQLHourlySessions(makeBrand({ apiVersion: "2024-01" }), DAY);
    assert.deepEqual(rows, [{ hour_of_day: 0, sessions: 1 }]);
    assert.equal(post.mock.calls[0].arguments[0], "https://test-shop.myshopify.com/admin/api/2025-10/graphql.json");
  });

  test("retries on 429 and returns [] on any other failure", async () => {
    const delays = fastTimers();
    queueAxiosPost(axios, [httpResp(429, {}, { "retry-after": "2" }), shopifyqlResp([])]);
    assert.deepEqual(await h.fetchShopifyQLHourlySessions(makeBrand(), DAY), []);
    assert.deepEqual(delays, [2000]);

    for (const resp of [
      httpResp(500),
      httpResp(200, { errors: [{}] }),
      httpResp(200, { data: {} }),
      shopifyqlResp([], { parseErrors: ["x"] }),
    ]) {
      mock.restoreAll();
      silenceConsole();
      queueAxiosPost(axios, [resp]);
      assert.deepEqual(await h.fetchShopifyQLHourlySessions(makeBrand(), DAY), []);
    }
  });
});

describe("upsertHourlySessionsSummary", () => {
  test("deletes the day then inserts 24 rows (zero-filled, adjusted=NULL)", async () => {
    const brand = makeBrand();
    await h.upsertHourlySessionsSummary(
      brand,
      [{ hour_of_day: 2, sessions: "7", sessions_with_cart_additions: "1" }, { hour_of_day: 99 }],
      DAY,
    );
    const [del, ins] = brand.pool.calls;
    assert.match(del.sql, /DELETE FROM hourly_sessions_summary_shopify WHERE date = \?/);
    assert.deepEqual(del.params, [DAY]);
    assert.equal(ins.params.length, 24 * 5);
    assert.deepEqual(ins.params.slice(10, 15), [DAY, 2, 7, 1, null]);
    assert.deepEqual(ins.params.slice(0, 5), [DAY, 0, 0, 0, null]);
    assert.equal(brand.pool.released, 1);
  });
});

describe("run", () => {
  test("ensures tables then backfills every date for every brand", async () => {
    process.env.TOTAL_CONFIG_COUNT = "2";
    fullBrandEnv(0);
    fullBrandEnv(1);
    queueAxiosPost(axios, [shopifyqlResp([{ hour_of_day: 5, sessions: 3 }])]);

    await h.run();

    assert.equal(pools.length, 2);
    for (const pool of pools) {
      assert.match(pool.calls[0].sql, /CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify/);
      const deleted = pool.calls.filter((c) => /^\s*DELETE/.test(c.sql)).map((c) => c.params[0]);
      assert.deepEqual(deleted, ["2024-06-01", "2024-06-02"]);
    }
  });
});
