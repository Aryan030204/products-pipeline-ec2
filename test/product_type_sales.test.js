import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import { ShopifyQLFetchError } from "../shopifyql_utils.js";
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

// Module-level constants read env at import time, so set env first.
setTestEnv({ SHOPIFYQL_FETCH_MAX_RETRIES: "2" });
const {
  ensureProductTypeSalesTables,
  fetchProductTypeSales,
  upsertProductTypeSalesSnapshot,
  refreshProductTypeFunnelRollup,
} = await import("../product_type_sales.js");

const DAY = "2024-06-01";

beforeEach(() => silenceConsole());
afterEach(() => mock.restoreAll());

describe("ensureProductTypeSalesTables", () => {
  test("creates both tables once, then short-circuits", async () => {
    const brand = makeBrand();
    await ensureProductTypeSalesTables(brand);
    assert.equal(brand.pool.calls.length, 2);
    assert.match(brand.pool.calls[0].sql, /CREATE TABLE IF NOT EXISTS product_type_sales_snapshot/);
    assert.match(brand.pool.calls[1].sql, /CREATE TABLE IF NOT EXISTS mv_product_type_funnel_daily/);
    assert.equal(brand._productTypeSalesTablesEnsured, true);
    assert.equal(brand.pool.released, 1);

    await ensureProductTypeSalesTables(brand);
    assert.equal(brand.pool.calls.length, 2, "no queries on second call");
  });
});

describe("fetchProductTypeSales", () => {
  test("posts a combined WITH clause query and keeps only real day rows", async () => {
    const post = queueAxiosPost(axios, [
      shopifyqlResp([
        { product_type: "Shoes", day: `${DAY}T00:00:00`, net_sales: "10" },
        { product_type: "", day: DAY }, // GROUP_TOTALS row
        { product_type: "Hats", day: null }, // TOTALS row
        { product_type: "Bags", day: "2024-05-31" }, // wrong day
      ]),
    ]);
    const brand = makeBrand();
    const rows = await fetchProductTypeSales(brand, DAY);

    assert.deepEqual(rows, [{ product_type: "Shoes", day: `${DAY}T00:00:00`, net_sales: "10" }]);
    const [url, body, opts] = post.mock.calls[0].arguments;
    assert.equal(url, "https://test-shop.myshopify.com/admin/api/2025-10/graphql.json");
    assert.equal(opts.headers["X-Shopify-Access-Token"], "tok");
    assert.match(body.query, /WITH GROUP_TOTALS, TOTALS, TIMEZONE 'Asia\/Kolkata'/);
    assert.match(body.query, /SINCE 2024-06-01T00:00:00 UNTIL 2024-06-01T23:59:59/);
    assert.match(body.query, /FROM sales/);
  });

  test("429 does not consume a retry attempt", async () => {
    const delays = fastTimers();
    const post = queueAxiosPost(axios, [
      httpResp(429, {}, { "retry-after": "2" }),
      httpResp(429, {}, {}),
      httpResp(429, {}, {}),
      shopifyqlResp([]),
    ]);
    const rows = await fetchProductTypeSales(makeBrand(), DAY);
    assert.deepEqual(rows, []);
    assert.equal(post.mock.callCount(), 4);
    assert.deepEqual(delays.slice(0, 3), [2000, 3000, 3000]);
  });

  test("THROTTLED GraphQL errors sleep until reset without consuming an attempt", async () => {
    fastTimers();
    const resetAt = new Date(Date.now() + 1000).toISOString();
    const throttled = httpResp(200, {
      errors: [{ extensions: { code: "THROTTLED", cost: { windowResetAt: resetAt } } }],
    });
    const post = queueAxiosPost(axios, [throttled, throttled, throttled, shopifyqlResp([])]);
    await fetchProductTypeSales(makeBrand(), DAY);
    assert.equal(post.mock.callCount(), 4);
  });

  const failingCases = [
    ["5xx", httpResp(503)],
    ["non-200", httpResp(403)],
    ["non-throttle GraphQL errors", httpResp(200, { errors: [{ message: "bad" }] })],
    ["missing payload", httpResp(200, { data: {} })],
    ["parseErrors", shopifyqlResp([], { parseErrors: ["syntax"] })],
  ];
  for (const [label, resp] of failingCases) {
    test(`throws ShopifyQLFetchError after retries on ${label}`, async () => {
      const delays = fastTimers();
      const post = queueAxiosPost(axios, [resp]);
      await assert.rejects(fetchProductTypeSales(makeBrand(), DAY), ShopifyQLFetchError);
      assert.equal(post.mock.callCount(), 2, "MAX_RETRIES=2");
      assert.deepEqual(delays, [500, 1000], "linear backoff");
    });
  }

  test("recovers when a retry succeeds", async () => {
    fastTimers();
    queueAxiosPost(axios, [httpResp(500), shopifyqlResp([{ product_type: "A", day: DAY }])]);
    const rows = await fetchProductTypeSales(makeBrand(), DAY);
    assert.equal(rows.length, 1);
  });
});

describe("upsertProductTypeSalesSnapshot", () => {
  test("no-op (but releases conn) for empty rows", async () => {
    const brand = makeBrand();
    await upsertProductTypeSalesSnapshot(brand, [], DAY);
    assert.equal(brand.pool.calls.length, 0);
    assert.equal(brand.pool.released, 1);
  });

  test("inserts normalized rows with one placeholder group per row", async () => {
    const brand = makeBrand();
    await upsertProductTypeSalesSnapshot(
      brand,
      [
        { product_type: " Shoes ", orders: "3", gross_sales: "100.5", net_sales: "90" },
        { product_type: "   ", orders: null },
      ],
      DAY,
    );
    const { sql, params } = brand.pool.calls[0];
    assert.match(sql, /INSERT INTO product_type_sales_snapshot/);
    assert.equal((sql.match(/\(\?, \?, \?, \?, \?\)/g) || []).length, 2);
    assert.deepEqual(params, [DAY, "Shoes", 3, 100.5, 90, DAY, "Unknown", 0, 0, 0]);
    assert.equal(brand.pool.released, 1);
  });
});

describe("refreshProductTypeFunnelRollup", () => {
  test("runs session seed, sales seed, then conversion recompute for the date", async () => {
    const brand = makeBrand();
    await refreshProductTypeFunnelRollup(brand, DAY);
    const calls = brand.pool.calls;
    assert.equal(calls.length, 3);
    assert.match(calls[0].sql, /FROM mv_product_sessions_by_type_daily/);
    assert.match(calls[1].sql, /FROM product_type_sales_snapshot/);
    assert.match(calls[2].sql, /UPDATE mv_product_type_funnel_daily/);
    for (const c of calls) assert.deepEqual(c.params, [DAY]);
    assert.equal(brand.pool.released, 1);
  });

  test("releases the connection when a query fails", async () => {
    const brand = makeBrand();
    brand.pool = makeFakePool(() => {
      throw new Error("db down");
    });
    await assert.rejects(refreshProductTypeFunnelRollup(brand, DAY), /db down/);
    assert.equal(brand.pool.released, 1);
  });
});
