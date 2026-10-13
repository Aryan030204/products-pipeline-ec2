// HTTP-facing functions in pipeline.js (Shopify REST + ShopifyQL), with axios mocked.
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import { ShopifyQLFetchError } from "../shopifyql_utils.js";
import {
  setTestEnv,
  makeBrand,
  httpResp,
  shopifyqlResp,
  queueAxiosPost,
  fastTimers,
  silenceConsole,
} from "./helpers.js";

// Small page size / page cap / retry count keep pagination and retry tests tiny.
setTestEnv({
  SHOPIFYQL_PAGE_SIZE: "2",
  SHOPIFYQL_MAX_PAGES: "3",
  SHOPIFYQL_FETCH_MAX_RETRIES: "2",
  SHOPIFYQL_HOURLY_DIM_MAX_RETRIES: "2",
});
const p = await import("../pipeline.js");

const DAY = "2024-06-01";

let delays;
beforeEach(() => {
  silenceConsole();
  delays = fastTimers();
});
afterEach(() => mock.restoreAll());

const queryOf = (call) => call.arguments[1].query;

describe("syncProductsForBrand", () => {
  const product = (id) => ({ id, handle: `h${id}`, status: "active", title: `T${id}`, product_type: "Shoes" });

  test("follows Link rel=next pagination and upserts each page", async () => {
    const get = mock.method(axios, "get", async (url) =>
      url.includes("page_info=2")
        ? httpResp(200, { products: [product(3)] }, {})
        : httpResp(200, { products: [product(1), product(2)] }, {
            link: '<https://x/prev>; rel="previous", <https://test-shop.myshopify.com/next?page_info=2>; rel="next"',
          }),
    );
    const brand = makeBrand();
    await p.syncProductsForBrand(brand);

    assert.equal(get.mock.callCount(), 2);
    assert.match(get.mock.calls[0].arguments[0], /^https:\/\/test-shop\.myshopify\.com\/admin\/api\/2025-10\/products\.json\?limit=250/);
    assert.equal(get.mock.calls[0].arguments[1].headers["X-Shopify-Access-Token"], "tok");
    assert.equal(get.mock.calls[1].arguments[0], "https://test-shop.myshopify.com/next?page_info=2");

    const inserts = brand.pool.calls;
    assert.equal(inserts.length, 2);
    assert.match(inserts[0].sql, /INSERT INTO product_landing_mapping/);
    assert.deepEqual(inserts[0].params.slice(0, 5), [1, "/products/h1", "active", "T1", "Shoes"]);
    assert.equal(inserts[1].params.length, 5);
    assert.equal(brand.pool.released, 1);
  });

  test("sleeps on 429 using Retry-After and retries the same page", async () => {
    let n = 0;
    mock.method(axios, "get", async () =>
      ++n === 1 ? httpResp(429, {}, { "retry-after": "4" }) : httpResp(200, { products: [product(1)] }),
    );
    const brand = makeBrand();
    await p.syncProductsForBrand(brand);
    assert.ok(delays.includes(4000));
    assert.equal(brand.pool.calls.length, 1);
  });

  test("stops on non-200 or empty pages without writing", async () => {
    for (const resp of [httpResp(500, "err"), httpResp(200, { products: [] })]) {
      mock.method(axios, "get", async () => resp);
      const brand = makeBrand();
      await p.syncProductsForBrand(brand);
      assert.equal(brand.pool.calls.length, 0);
      assert.equal(brand.pool.released, 1);
    }
  });

  test("swallows request errors (logged) and still releases", async () => {
    mock.method(axios, "get", async () => {
      throw new Error("network");
    });
    const brand = makeBrand();
    await p.syncProductsForBrand(brand);
    assert.equal(brand.pool.released, 1);
    assert.ok(console.error.mock.calls.some((c) => /Product sync failed/.test(c.arguments[0])));
  });
});

describe("fetchShopifyQLSessions", () => {
  test("paginates with LIMIT/OFFSET until a short page", async () => {
    const post = queueAxiosPost(axios, [
      shopifyqlResp([{ a: 1 }, { a: 2 }]),
      shopifyqlResp([{ a: 3 }]),
    ]);
    const rows = await p.fetchShopifyQLSessions(makeBrand(), DAY);
    assert.deepEqual(rows, [{ a: 1 }, { a: 2 }, { a: 3 }]);
    assert.equal(post.mock.callCount(), 2);
    assert.match(queryOf(post.mock.calls[0]), /LIMIT 2 /);
    assert.doesNotMatch(queryOf(post.mock.calls[0]), /OFFSET/);
    assert.match(queryOf(post.mock.calls[1]), /LIMIT 2 OFFSET 2/);
    assert.equal(post.mock.calls[0].arguments[0], "https://test-shop.myshopify.com/admin/api/2025-10/graphql.json");
  });

  test("returns [] for an empty day", async () => {
    queueAxiosPost(axios, [shopifyqlResp([])]);
    assert.deepEqual(await p.fetchShopifyQLSessions(makeBrand(), DAY), []);
  });

  test("throws when pagination never ends within SHOPIFYQL_MAX_PAGES", async () => {
    const post = queueAxiosPost(axios, [shopifyqlResp([{ a: 1 }, { a: 2 }])]);
    await assert.rejects(p.fetchShopifyQLSessions(makeBrand(), DAY), (err) => {
      assert.ok(err instanceof ShopifyQLFetchError);
      assert.match(err.message, /exceeded max pages \(3 x 2\)/);
      return true;
    });
    assert.equal(post.mock.callCount(), 3);
  });

  test("throws (whole day skipped) when a later page exhausts its retries", async () => {
    const post = queueAxiosPost(axios, [shopifyqlResp([{ a: 1 }, { a: 2 }]), httpResp(502)]);
    await assert.rejects(p.fetchShopifyQLSessions(makeBrand(), DAY), /page 1 \(offset 2\) failed after 2 attempts/);
    assert.equal(post.mock.callCount(), 3);
  });

  for (const [label, resp] of [
    ["non-200", httpResp(404)],
    ["GraphQL errors", httpResp(200, { errors: [{ message: "x" }] })],
    ["missing payload", httpResp(200, { data: {} })],
    ["parseErrors", shopifyqlResp([], { parseErrors: ["bad"] })],
  ]) {
    test(`retries with backoff then throws on ${label}`, async () => {
      queueAxiosPost(axios, [resp]);
      await assert.rejects(p.fetchShopifyQLSessions(makeBrand(), DAY), ShopifyQLFetchError);
      assert.deepEqual(delays, [500, 1000]);
    });
  }

  test("429 and THROTTLED responses do not consume attempts", async () => {
    const throttled = httpResp(200, {
      errors: [{ extensions: { code: "THROTTLED", cost: { windowResetAt: new Date(Date.now() + 500).toISOString() } } }],
    });
    const post = queueAxiosPost(axios, [
      httpResp(429, {}, {}),
      throttled,
      httpResp(429, {}, {}),
      throttled,
      shopifyqlResp([{ a: 1 }]),
    ]);
    assert.deepEqual(await p.fetchShopifyQLSessions(makeBrand(), DAY), [{ a: 1 }]);
    assert.equal(post.mock.callCount(), 5);
  });
});

describe("fetchShopifyQLHourlySessions", () => {
  test("returns formatted rows on success", async () => {
    const post = queueAxiosPost(axios, [
      shopifyqlResp([[0, 5]], { columns: [{ name: "hour_of_day" }, { name: "sessions" }] }),
    ]);
    assert.deepEqual(await p.fetchShopifyQLHourlySessions(makeBrand(), DAY), [{ hour_of_day: 0, sessions: 5 }]);
    assert.match(queryOf(post.mock.calls[0]), /GROUP BY hour_of_day/);
  });

  test("retries 429 indefinitely until success", async () => {
    const post = queueAxiosPost(axios, [
      httpResp(429, {}, { "retry-after": "1" }),
      httpResp(429, {}, {}),
      shopifyqlResp([{ hour_of_day: 1 }]),
    ]);
    assert.equal((await p.fetchShopifyQLHourlySessions(makeBrand(), DAY)).length, 1);
    assert.equal(post.mock.callCount(), 3);
    assert.deepEqual(delays, [1000, 3000]);
  });

  for (const [label, resp] of [
    ["non-200", httpResp(500)],
    ["GraphQL errors", httpResp(200, { errors: [{}] })],
    ["missing payload", httpResp(200, { data: {} })],
    ["parseErrors", shopifyqlResp([], { parseErrors: ["x"] })],
  ]) {
    test(`returns [] without retrying on ${label}`, async () => {
      const post = queueAxiosPost(axios, [resp]);
      assert.deepEqual(await p.fetchShopifyQLHourlySessions(makeBrand(), DAY), []);
      assert.equal(post.mock.callCount(), 1);
    });
  }
});

describe("fetchHourlyDimensionSessions", () => {
  const isAllHours = (call) => /hour_of_day/.test(queryOf(call));
  const hourOf = (call) => Number(queryOf(call).match(/T(\d{2}):00:00 UNTIL/)[1]);

  test("fast path: single all-hours query under the LIMIT, invalid hours dropped", async () => {
    const post = queueAxiosPost(axios, [
      shopifyqlResp([
        { hour_of_day: "3", sessions: 1 },
        { hour_of_day: 23, sessions: 2 },
        { hour_of_day: 24, sessions: 3 },
      ]),
    ]);
    const rows = await p.fetchHourlyDimensionSessions(makeBrand(), DAY);
    assert.equal(post.mock.callCount(), 1);
    assert.ok(isAllHours(post.mock.calls[0]));
    assert.deepEqual(rows, [
      { hour_of_day: "3", sessions: 1, hour: 3 },
      { hour_of_day: 23, sessions: 2, hour: 23 },
    ]);
  });

  for (const [label, allHoursResp] of [
    ["returns 0 rows", shopifyqlResp([])],
    ["hits LIMIT 1000", shopifyqlResp(Array.from({ length: 1000 }, () => ({ hour_of_day: 1 })))],
    ["fails with a non-200", httpResp(403)],
  ]) {
    test(`falls back to 24 hourly slices when the single query ${label}`, async () => {
      const post = mock.method(axios, "post", async (_url, body) =>
        /hour_of_day/.test(body.query) ? allHoursResp : shopifyqlResp([{ landing_page_path: "/x" }]),
      );
      const rows = await p.fetchHourlyDimensionSessions(makeBrand(), DAY);
      const slices = post.mock.calls.filter((c) => !isAllHours(c));
      assert.deepEqual(slices.map(hourOf), Array.from({ length: 24 }, (_, h) => h));
      assert.equal(rows.length, 24);
      assert.deepEqual(rows.map((r) => r.hour), Array.from({ length: 24 }, (_, h) => h));
    });
  }

  test("falls back when the single query throws", async () => {
    mock.method(axios, "post", async (_url, body) => {
      if (/hour_of_day/.test(body.query)) throw new Error("socket hang up");
      return shopifyqlResp([]);
    });
    const rows = await p.fetchHourlyDimensionSessions(makeBrand(), DAY);
    assert.deepEqual(rows, []);
    assert.ok(console.warn.mock.calls.some((c) => /socket hang up/.test(c.arguments[0])));
  });

  test("skips hours that exhaust retries and flags truncated hours", async () => {
    const post = mock.method(axios, "post", async (_url, body) => {
      if (/hour_of_day/.test(body.query)) return shopifyqlResp([]);
      const h = Number(body.query.match(/T(\d{2}):00:00 UNTIL/)[1]);
      if (h === 5) return httpResp(503);
      if (h === 6) return shopifyqlResp(Array.from({ length: 1000 }, () => ({ landing_page_path: "/t" })));
      return shopifyqlResp([]);
    });
    const rows = await p.fetchHourlyDimensionSessions(makeBrand(), DAY);
    assert.equal(rows.length, 1000);
    assert.ok(rows.every((r) => r.hour === 6));
    assert.equal(post.mock.calls.filter((c) => !isAllHours(c) && hourOf(c) === 5).length, 2, "MAX_RETRIES=2");
    const warnings = console.warn.mock.calls.map((c) => c.arguments[0]).join("\n");
    assert.match(warnings, /PARTIAL .* Skipped hours: 5/);
    assert.match(warnings, /TRUNCATED .* 1000-row slices: 6/);
  });
});
