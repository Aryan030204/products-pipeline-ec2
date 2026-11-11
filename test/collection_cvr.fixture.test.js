// Opt-in integration test: runs the EXACT production SELECT of the collection
// MV against inline fixture tables (CTEs that shadow product_sessions_snapshot
// and shopify_orders), so it reads and writes no real table. Skipped unless a
// MySQL 8 server is configured:
//   COLLECTION_CVR_FIXTURE_DB='{"host":"...","port":3306,"user":"...","password":"...","database":"..."}'
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import mysql from "mysql2/promise";
import { setTestEnv } from "./helpers.js";

setTestEnv();
const p = await import("../pipeline.js");

const dbConfig = process.env.COLLECTION_CVR_FIXTURE_DB
  ? JSON.parse(process.env.COLLECTION_CVR_FIXTURE_DB)
  : null;

const DAY = "2026-04-01";
const OTHER_DAY = "2026-04-02";

function sessionRow(date, type, path, sessions) {
  return `SELECT CAST('${date}' AS DATE) AS date, '${type}' AS landing_page_type, '${path}' AS landing_page_path_norm, ${sessions} AS sessions`;
}
function orderRow(date, orderId, path) {
  return `SELECT CAST('${date}' AS DATE) AS created_dt, '${orderId}' AS order_id, '${path}' AS landing_page_path`;
}

// The production statement is INSERT ... SELECT ... ON DUPLICATE KEY UPDATE;
// only the SELECT is run.
function productionSelect() {
  const sql = p.COLLECTION_MV_UPSERT_SQL;
  return sql.slice(sql.indexOf("SELECT"), sql.indexOf("ON DUPLICATE KEY UPDATE"));
}

async function runWithFixtures(conn, sessions, orders) {
  const sql = `
    WITH
      product_sessions_snapshot AS (${sessions.join(" UNION ALL ")}),
      shopify_orders AS (${orders.join(" UNION ALL ")})
    ${productionSelect()}`;
  const [rows] = await conn.query(sql, [DAY, DAY]);
  return rows
    .map((r) => ({
      path: r.collection_path,
      sessions: Number(r.sessions),
      orders: Number(r.orders),
      cvr: Number(r.conversion_rate_pct),
    }))
    .sort((a, b) => a.path.localeCompare(b.path));
}

describe("collection MV SELECT against fixture data", { skip: !dbConfig }, () => {
  let conn;
  before(async () => {
    conn = await mysql.createConnection({ ...dbConfig, ssl: { rejectUnauthorized: false } });
  });
  after(async () => {
    if (conn) await conn.end();
  });

  test("1000 sessions and 50 distinct orders give a 5% CVR", async () => {
    const sessions = [sessionRow(DAY, "Collection", "/collections/shoes", 1000)];
    // 50 orders, each with 2 line-item rows -> 100 rows must count as 50 orders
    const orders = [];
    for (let i = 1; i <= 50; i += 1) {
      orders.push(orderRow(DAY, `o${i}`, "/collections/shoes"));
      orders.push(orderRow(DAY, `o${i}`, "/collections/shoes"));
    }
    const result = await runWithFixtures(conn, sessions, orders);
    assert.deepEqual(result, [{ path: "/collections/shoes", sessions: 1000, orders: 50, cvr: 5 }]);
  });

  test("UTM/referrer rows of the same collection+date aggregate into one session total", async () => {
    const sessions = [
      sessionRow(DAY, "Collection", "/collections/shoes", 400), // e.g. utm_source=instagram
      sessionRow(DAY, "Collection", "/collections/shoes", 350), // e.g. utm_source=google
      sessionRow(DAY, "Collection", "/collections/Shoes", 250), // e.g. direct; different case
    ];
    const orders = [orderRow(DAY, "a", "/collections/shoes")];
    const result = await runWithFixtures(conn, sessions, orders);
    assert.equal(result.length, 1, "one collection must be one row, not one per UTM");
    assert.equal(result[0].sessions, 1000);
  });

  test("only exact /collections/<handle> counts; collection-product, tag and root paths do not", async () => {
    const sessions = [
      sessionRow(DAY, "Collection", "/collections/shoes", 100),
      sessionRow(DAY, "Product", "/collections/shoes/products/nike", 500), // Shopify types this as Product
      sessionRow(DAY, "Collection", "/collections/shoes/red", 30), // extra segment
      sessionRow(DAY, "Collection", "/collections/", 50), // not a collection
      sessionRow(DAY, "Homepage", "/", 900),
    ];
    const orders = [
      orderRow(DAY, "1", "/collections/shoes"),
      orderRow(DAY, "2", "/collections/shoes/products/nike"), // not a collection landing
      orderRow(DAY, "3", "/products/nike"),
      orderRow(DAY, "4", "/"),
    ];
    const result = await runWithFixtures(conn, sessions, orders);
    assert.deepEqual(result, [{ path: "/collections/shoes", sessions: 100, orders: 1, cvr: 1 }]);
  });

  test("date boundaries, no-order collections, and orders without sessions", async () => {
    const sessions = [
      sessionRow(DAY, "Collection", "/collections/hats", 200), // sessions, no orders
      sessionRow(DAY, "Collection", "/collections/sale", 100), // orders only on another day
      sessionRow(OTHER_DAY, "Collection", "/collections/hats", 999), // other day: excluded
    ];
    const orders = [
      orderRow(OTHER_DAY, "x1", "/collections/sale"),
      orderRow(OTHER_DAY, "x2", "/collections/sale"),
      orderRow(DAY, "g1", "/collections/ghost"), // orders but no sessions -> no row
    ];
    const result = await runWithFixtures(conn, sessions, orders);
    assert.deepEqual(result, [
      { path: "/collections/hats", sessions: 200, orders: 0, cvr: 0 },
      { path: "/collections/sale", sessions: 100, orders: 0, cvr: 0 },
    ]);
  });
});
