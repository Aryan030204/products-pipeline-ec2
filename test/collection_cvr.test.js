// Collection/PLP CVR materialized view (V1): query shape and failure isolation.
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import {
  setTestEnv,
  makeBrand,
  makeFakePool,
  defaultQueryHandler,
  silenceConsole,
} from "./helpers.js";

setTestEnv();
const p = await import("../pipeline.js");

const DAY = "2024-06-01";

beforeEach(() => silenceConsole());
afterEach(() => mock.restoreAll());

// Split the statement into its sessions / orders / outer parts for assertions.
function parts(sql) {
  const sessionsStart = sql.indexOf("LOWER(landing_page_path_norm) AS collection_path");
  const ordersStart = sql.indexOf("created_dt AS created_date");
  return {
    outer: sql.slice(0, sessionsStart),
    sessions: sql.slice(sessionsStart, ordersStart),
    orders: sql.slice(ordersStart, sql.indexOf("ON DUPLICATE KEY UPDATE")),
  };
}

describe("COLLECTION_PATH_REGEX", () => {
  const re = new RegExp(p.COLLECTION_PATH_REGEX);

  test("matches exactly /collections/<handle>", () => {
    for (const path of ["/collections/shoes", "/collections/gifts-for-men", "/collections/all"]) {
      assert.ok(re.test(path), path);
    }
  });

  test("rejects everything that is not a collection landing", () => {
    for (const path of [
      "/collections/shoes/products/nike-air", // product landing inside a collection
      "/collections/shoes/red", // extra segment
      "/collections/",
      "/collections",
      "/products/nike-air",
      "/",
      "/cart",
      "/checkouts/abc",
      "/pages/collections/shoes",
    ]) {
      assert.ok(!re.test(path), path);
    }
  });
});

describe("COLLECTION_MV_UPSERT_SQL", () => {
  const { outer, sessions, orders } = parts(p.COLLECTION_MV_UPSERT_SQL);

  test("upserts date + collection_path + sessions + orders + cvr into the new MV", () => {
    assert.match(outer, /INSERT INTO mv_collection_sessions_by_path_daily/);
    assert.match(outer, /\(date, collection_path, sessions, orders, conversion_rate_pct\)/);
    assert.match(p.COLLECTION_MV_UPSERT_SQL, /ON DUPLICATE KEY UPDATE/);
  });

  test("CVR is orders / sessions * 100 rounded to 4dp, 0 when there are no sessions", () => {
    assert.match(outer, /WHEN s\.sessions > 0/);
    assert.match(outer, /ROUND\(COALESCE\(o\.orders, 0\) \/ s\.sessions \* 100, 4\)/);
  });

  test("sessions: Collection landing rows only, exact-collection path, one total per date+path", () => {
    assert.match(sessions, /landing_page_type = 'Collection'/);
    assert.ok(sessions.includes(`REGEXP '${p.COLLECTION_PATH_REGEX}'`));
    assert.match(sessions, /SUM\(sessions\) AS sessions/);
    // grouping must NOT include the UTM / referrer dimensions, otherwise one
    // collection would be split into several rows.
    const groupBy = sessions.slice(sessions.indexOf("GROUP BY"));
    assert.match(groupBy, /GROUP BY date, LOWER\(landing_page_path_norm\)/);
    for (const dim of ["utm_", "referrer"]) {
      assert.ok(!groupBy.includes(dim), `grouping must not include ${dim}`);
    }
  });

  test("orders: COUNT(DISTINCT order_id) by normalized landing path, never line items", () => {
    assert.match(orders, /COUNT\(DISTINCT order_id\) AS orders/);
    assert.ok(!/COUNT\(\*\)/.test(orders));
    assert.match(orders, /landing_page_path REGEXP/);
    assert.ok(orders.includes(`REGEXP '${p.COLLECTION_PATH_REGEX}'`));
    assert.match(orders, /WHERE created_dt = \?/); // same day column as the PDP path MV
    assert.match(orders, /GROUP BY created_dt, landing_page_path/);
    assert.ok(!/LIKE '%collections/.test(p.COLLECTION_MV_UPSERT_SQL), "must not use a LIKE '%collections/%' test");
  });

  test("does not touch product_id attribution (collection rows have no product)", () => {
    assert.ok(!/product_id/.test(p.COLLECTION_MV_UPSERT_SQL));
    assert.ok(!/product_landing_mapping/.test(p.COLLECTION_MV_UPSERT_SQL));
  });
});

describe("refreshMaterializedViews isolation", () => {
  test("a collection MV failure is warned about and does not fail the PDP views", async () => {
    const pool = makeFakePool((sql) => {
      if (/INSERT INTO mv_collection_sessions_by_path_daily/.test(sql)) {
        throw new Error("Unknown column 'landing_page_path' in 'where clause'");
      }
      return defaultQueryHandler(sql);
    });
    const brand = makeBrand({ pool });

    await p.refreshMaterializedViews(brand, DAY); // must not throw

    assert.equal(pool.calls.length, 4);
    assert.equal(pool.released, 1);
    assert.ok(
      console.warn.mock.calls.some((c) => /Collection MV refresh skipped/.test(c.arguments[0])),
    );
  });

  test("a PDP view failure still rethrows (unchanged behavior)", async () => {
    const pool = makeFakePool((sql) => {
      if (/INSERT INTO mv_product_sessions_by_path_daily/.test(sql)) throw new Error("path mv fail");
      return defaultQueryHandler(sql);
    });
    await assert.rejects(p.refreshMaterializedViews(makeBrand({ pool }), DAY), /path mv fail/);
    assert.equal(pool.released, 1);
  });
});
