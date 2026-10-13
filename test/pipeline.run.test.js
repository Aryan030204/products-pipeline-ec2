// Orchestration in normal (hourly) mode: processBrand, runPipelineForDate, safeRun.
import { test, describe, before, beforeEach, afterEach, mock } from "node:test";
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
  HOURLY_PRODUCT_SESSIONS_ENABLED: "true",
  HOURLY_BRAND_CONCURRENCY: "2",
  PIPELINE_ID: "pipe-1",
});
delete process.env.BACKFILL_MODE;
const p = await import("../pipeline.js");

const DAY = "2024-06-01";
const STARTED = "https://manager.test/started";
const COMPLETED = "https://manager.test/completed";
const FAILED = "https://manager.test/failed";

// Asserts each pattern appears in the SQL log, in this order.
function assertSqlOrder(log, patterns) {
  let from = 0;
  for (const re of patterns) {
    const idx = log.findIndex((s, i) => i >= from && re.test(s));
    assert.ok(idx >= 0, `expected ${re} after position ${from}`);
    from = idx + 1;
  }
}

const managerPosts = () =>
  axios.post.mock.calls.filter((c) => c.arguments[0].startsWith("https://manager.test"));

// getBrands() caches for the module's lifetime, so load the brands once.
const handlers = { current: defaultQueryHandler };
let brands;
before(async () => {
  silenceConsole();
  mockBrandsApi(axios, mysql, { 1: apiBrand(1), 2: apiBrand(2) }, handlers);
  brands = await p.getBrands();
  mock.restoreAll();
});

beforeEach(() => {
  silenceConsole();
  fastTimers();
  handlers.current = defaultQueryHandler;
  for (const b of brands) b.pool.calls = [];
  mock.method(axios, "get", async () => httpResp(200, { products: [] }));
  for (const key of ["MANAGER_STARTED_ENDPOINT", "MANAGER_COMPLETED_ENDPOINT", "MANAGER_FAILED_ENDPOINT"]) {
    delete process.env[key];
  }
});
afterEach(() => mock.restoreAll());

describe("processBrand", () => {
  test("runs every stage in order for the requested date", async () => {
    mockShopifyQL(axios);
    const brand = makeBrand();
    await p.processBrand(brand, DAY);

    assertSqlOrder(sqlLog(brand.pool), [
      /CREATE TABLE IF NOT EXISTS product_sessions_snapshot/,
      /SELECT key_value FROM pipeline_metadata/,
      /INSERT INTO product_landing_mapping|INSERT INTO pipeline_metadata/,
      /INSERT INTO product_sessions_snapshot/,
      /INSERT INTO mv_product_sessions_by_type_daily/,
      /CREATE TABLE IF NOT EXISTS product_type_sales_snapshot/,
      /INSERT INTO product_type_sales_snapshot/,
      /INSERT INTO mv_product_type_funnel_daily/,
      /INSERT INTO hourly_sessions_summary_shopify/,
      /INSERT INTO hourly_product_sessions/,
      /INSERT INTO hourly_product_performance_rollup/,
    ]);
    const snapshot = brand.pool.calls.find((c) => /INSERT INTO product_sessions_snapshot/.test(c.sql));
    assert.equal(snapshot.params[0], DAY);
    assert.equal(axios.get.mock.callCount(), 1, "product sync ran");
  });

  test("defaults to brand-local today when no date is given", async () => {
    const post = mockShopifyQL(axios);
    const brand = makeBrand();
    await p.processBrand(brand, null);
    assert.ok(post.mock.calls.every((c) => /DURING today/.test(c.arguments[1].query)));
    const snapshot = brand.pool.calls.find((c) => /INSERT INTO product_sessions_snapshot/.test(c.sql));
    assert.equal(snapshot.params[0], todayYMDInTimeZone("Asia/Kolkata"));
  });

  test("skips product sync when it already ran today", async () => {
    mockShopifyQL(axios);
    const brand = makeBrand({
      pool: makeFakePool((sql) =>
        /SELECT key_value FROM pipeline_metadata/.test(sql)
          ? [[{ key_value: todayYMDInTimeZone("Asia/Kolkata") }], []]
          : defaultQueryHandler(sql),
      ),
    });
    await p.processBrand(brand, DAY);
    assert.equal(axios.get.mock.callCount(), 0);
    assert.ok(!sqlLog(brand.pool).some((s) => /INSERT INTO pipeline_metadata/.test(s)));
  });

  test("skips the whole day (no writes) when the sessions fetch fails", async () => {
    mockShopifyQL(axios, { onPost: async () => httpResp(500) });
    const brand = makeBrand();
    await p.processBrand(brand, DAY);
    const log = sqlLog(brand.pool);
    assert.ok(!log.some((s) => /INSERT INTO (product_sessions_snapshot|mv_|hourly_)/.test(s)));
    assert.ok(console.error.mock.calls.some((c) => /Skipping 2024-06-01 entirely/.test(c.arguments[0])));
  });

  test("continues to hourly steps when only the product-type sales fetch fails", async () => {
    mockShopifyQL(axios, {
      onPost: async (_url, body) => (/FROM sales/.test(body.query) ? httpResp(500) : null),
    });
    const brand = makeBrand();
    await p.processBrand(brand, DAY);
    const log = sqlLog(brand.pool);
    assert.ok(!log.some((s) => /INSERT INTO product_type_sales_snapshot/.test(s)));
    assert.ok(log.some((s) => /INSERT INTO hourly_sessions_summary_shopify/.test(s)));
    assert.ok(log.some((s) => /INSERT INTO hourly_product_performance_rollup/.test(s)));
  });

  test("propagates non-ShopifyQL errors (e.g. DB failures)", async () => {
    mockShopifyQL(axios);
    const brand = makeBrand({
      pool: makeFakePool((sql) => {
        if (/INSERT INTO product_sessions_snapshot/.test(sql)) throw new Error("disk full");
        return defaultQueryHandler(sql);
      }),
    });
    await assert.rejects(p.processBrand(brand, DAY), /disk full/);
  });
});

describe("runPipelineForDate", () => {
  test("processes every loaded brand", async () => {
    mockShopifyQL(axios);
    await p.runPipelineForDate(DAY);
    for (const b of brands) {
      const snap = b.pool.calls.find((c) => /INSERT INTO product_sessions_snapshot/.test(c.sql));
      assert.ok(snap, `brand ${b.tag} processed`);
      assert.equal(snap.params[0], DAY);
    }
  });
});

describe("safeRun", () => {
  test("scheduled runs process all brands without contacting the manager", async () => {
    process.env.MANAGER_STARTED_ENDPOINT = STARTED;
    mockShopifyQL(axios);
    await p.safeRun("hourly");
    assert.equal(managerPosts().length, 0);
    for (const b of brands) {
      assert.ok(sqlLog(b.pool).some((s) => /INSERT INTO product_sessions_snapshot/.test(s)));
    }
  });

  test("manual runs report running then completed to the manager", async () => {
    process.env.MANAGER_STARTED_ENDPOINT = STARTED;
    process.env.MANAGER_COMPLETED_ENDPOINT = COMPLETED;
    mockShopifyQL(axios);
    await p.safeRun("manual_http", { runId: "r1" });

    const posts = managerPosts();
    assert.deepEqual(posts.map((c) => c.arguments[0]), [STARTED, COMPLETED]);
    assert.deepEqual(posts[0].arguments[1], { runId: "r1", pipelineId: "pipe-1", status: "running" });
    assert.deepEqual(posts[1].arguments[1], { runId: "r1", pipelineId: "pipe-1", status: "completed" });
    assert.deepEqual(posts[0].arguments[2].headers, { "x-pipeline-key": "hdr" });
  });

  test("manual runs without manager endpoints just warn", async () => {
    mockShopifyQL(axios);
    await p.safeRun("manual_http", { runId: "r2" });
    assert.equal(managerPosts().length, 0);
    assert.ok(console.warn.mock.calls.some((c) => /Skipping 'running' status report/.test(c.arguments[0])));
  });

  test("manager reporting failures do not fail the run", async () => {
    process.env.MANAGER_STARTED_ENDPOINT = STARTED;
    process.env.MANAGER_COMPLETED_ENDPOINT = COMPLETED;
    mockShopifyQL(axios, {
      onPost: async (url) => {
        if (url.startsWith("https://manager.test")) throw new Error("manager down");
      },
    });
    await p.safeRun("manual_http", { runId: "r3" });
    const warnings = console.warn.mock.calls.map((c) => c.arguments[0]).join("\n");
    assert.match(warnings, /Failed to report 'running' status: manager down/);
    assert.match(warnings, /Failed to report 'completed' status: manager down/);
  });

  test("crashes are caught and reported as failed with a derived error code", async () => {
    process.env.MANAGER_FAILED_ENDPOINT = FAILED;
    mockShopifyQL(axios);
    handlers.current = (sql) => {
      if (/INSERT INTO product_sessions_snapshot/.test(sql)) {
        throw Object.assign(new Error("Access denied for user"), { code: "ER_ACCESS_DENIED_ERROR" });
      }
      return defaultQueryHandler(sql);
    };
    await p.safeRun("manual_http", { runId: "r4" }); // resolves, does not reject

    const [failed] = managerPosts();
    assert.equal(failed.arguments[0], FAILED);
    const body = failed.arguments[1];
    assert.equal(body.pipelineId, "pipe-1");
    assert.equal(body.runId, "r4");
    assert.equal(body.errorCode, "DB_AUTH_FAILED");
    assert.equal(typeof body.errorBody, "string");
    assert.ok(body.errorBody.length > 0);
  });

  test("crash on a scheduled run is logged but not reported", async () => {
    process.env.MANAGER_FAILED_ENDPOINT = FAILED;
    mockShopifyQL(axios);
    handlers.current = () => {
      throw new Error("boom");
    };
    await p.safeRun("hourly");
    assert.equal(managerPosts().length, 0);
    assert.ok(console.error.mock.calls.some((c) => /Pipeline crashed \(hourly\)/.test(c.arguments[0])));
  });

  test("skips a run while another is in progress, then allows the next", async () => {
    mockShopifyQL(axios);
    const first = p.safeRun("hourly");
    await p.safeRun("manual_http", { runId: "overlap" });
    await first;
    assert.ok(console.log.mock.calls.some((c) => /Skipping run \(manual_http\)/.test(c.arguments[0])));

    console.log.mock.resetCalls();
    await p.safeRun("hourly");
    assert.ok(!console.log.mock.calls.some((c) => /Skipping run/.test(c.arguments[0])));
  });
});
