// DB-writing functions in pipeline.js, run against a fake mysql2 pool.
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
const RUN_STARTED = "2025-01-01 00:00:00";

beforeEach(() => silenceConsole());
afterEach(() => mock.restoreAll());

function fakeConn(rows) {
  const calls = [];
  return {
    calls,
    async query(sql, params) {
      calls.push({ sql, params });
      return [rows, []];
    },
  };
}

function sqlMatching(pool, re) {
  return pool.calls.filter((c) => re.test(c.sql));
}

describe("ensureIndex / ensureColumn", () => {
  test("ensureIndex adds the index only when SHOW INDEX finds none", async () => {
    const missing = fakeConn([]);
    await p.ensureIndex(missing, "t", "idx_x", "ALTER TABLE t ADD INDEX idx_x (x)");
    assert.deepEqual(missing.calls[0].params, ["t", "idx_x"]);
    assert.equal(missing.calls[1].sql, "ALTER TABLE t ADD INDEX idx_x (x)");

    const present = fakeConn([{ Key_name: "idx_x" }]);
    await p.ensureIndex(present, "t", "idx_x", "ALTER ...");
    assert.equal(present.calls.length, 1);
  });

  test("ensureColumn adds the column only when SHOW COLUMNS finds none", async () => {
    const missing = fakeConn([]);
    await p.ensureColumn(missing, "t", "c", "ALTER TABLE t ADD COLUMN c INT");
    assert.match(missing.calls[0].sql, /SHOW COLUMNS FROM \?\? LIKE \?/);
    assert.deepEqual(missing.calls[0].params, ["t", "c"]);
    assert.equal(missing.calls.length, 2);

    const present = fakeConn([{ Field: "c" }]);
    await p.ensureColumn(present, "t", "c", "ALTER ...");
    assert.equal(present.calls.length, 1);
  });
});

describe("ensureTablesForBrand", () => {
  test("is a no-op when tables were already ensured", async () => {
    const brand = makeBrand({ _tablesEnsured: true });
    await p.ensureTablesForBrand(brand);
    assert.equal(brand.pool.acquired, 0);
  });

  test("creates every table and skips migrations when schema is current", async () => {
    const brand = makeBrand();
    await p.ensureTablesForBrand(brand);

    for (const table of [
      "product_sessions_snapshot",
      "mv_product_sessions_by_type_daily",
      "mv_product_sessions_by_path_daily",
      "product_landing_mapping",
      "mv_product_sessions_by_campaign_daily",
      "pipeline_metadata",
      "hourly_sessions_summary_shopify",
      "hourly_product_sessions",
      "hourly_product_performance_rollup",
    ]) {
      assert.equal(
        sqlMatching(brand.pool, new RegExp(`CREATE TABLE IF NOT EXISTS ${table}\\b`)).length,
        1,
        table,
      );
    }
    assert.equal(sqlMatching(brand.pool, /^\s*ALTER/).length, 0, "nothing to migrate");
    assert.equal(brand._tablesEnsured, true);
    assert.equal(brand.pool.released, 1);
  });

  test("runs ALTER migrations when columns/indexes are missing", async () => {
    const pool = makeFakePool((sql) =>
      /^\s*SHOW/.test(sql) ? [[], []] : defaultQueryHandler(sql),
    );
    const brand = makeBrand({ pool });
    await p.ensureTablesForBrand(brand);

    const alters = sqlMatching(pool, /^\s*ALTER/).map((c) => c.sql);
    assert.ok(alters.some((s) => /ADD COLUMN product_id/.test(s)));
    assert.ok(alters.some((s) => /ADD COLUMN product_title/.test(s)));
    assert.ok(alters.some((s) => /DROP PRIMARY KEY, ADD PRIMARY KEY \(date, landing_page_type, product_type\)/.test(s)));
    assert.ok(alters.some((s) => /product_landing_mapping ADD COLUMN product_type/.test(s)));
    assert.equal(brand._tablesEnsured, true);
  });

  test("tolerates a failing MV primary-key migration (warns, continues)", async () => {
    const pool = makeFakePool((sql) => {
      if (/^\s*SHOW COLUMNS FROM mv_product_sessions_by_type_daily/.test(sql)) return [[], []];
      if (/DROP PRIMARY KEY/.test(sql)) throw new Error("pk clash");
      return defaultQueryHandler(sql);
    });
    const brand = makeBrand({ pool });
    await p.ensureTablesForBrand(brand);
    assert.equal(brand._tablesEnsured, true);
    assert.ok(console.warn.mock.calls.some((c) => /pk clash/.test(c.arguments[0])));
  });

  test("releases the connection and leaves the flag unset on failure", async () => {
    const pool = makeFakePool(() => {
      throw new Error("db down");
    });
    const brand = makeBrand({ pool });
    await assert.rejects(p.ensureTablesForBrand(brand), /db down/);
    assert.equal(brand._tablesEnsured, false);
    assert.equal(pool.released, 1);
  });
});

describe("product sync metadata", () => {
  test("getLastProductSyncDate returns null when unset", async () => {
    const brand = makeBrand();
    assert.equal(await p.getLastProductSyncDate(brand), null);
    assert.equal(brand.pool.released, 1);
  });

  test("getLastProductSyncDate returns the stored value", async () => {
    const pool = makeFakePool(() => [[{ key_value: DAY }], []]);
    assert.equal(await p.getLastProductSyncDate(makeBrand({ pool })), DAY);
  });

  test("setLastProductSyncDate upserts the date", async () => {
    const brand = makeBrand();
    await p.setLastProductSyncDate(brand, DAY);
    assert.match(brand.pool.calls[0].sql, /INSERT INTO pipeline_metadata/);
    assert.deepEqual(brand.pool.calls[0].params, [DAY]);
    assert.equal(brand.pool.released, 1);
  });
});

describe("upsertProductSessionsSnapshot", () => {
  test("does nothing for empty rows (but releases the connection)", async () => {
    const brand = makeBrand();
    await p.upsertProductSessionsSnapshot(brand, [], DAY);
    assert.equal(brand.pool.calls.length, 0);
    assert.equal(brand.pool.released, 1);
  });

  test("inserts normalized rows, backfills product ids, then sweeps stale rows", async () => {
    const brand = makeBrand();
    await p.upsertProductSessionsSnapshot(
      brand,
      [
        {
          landing_page_type: "Product",
          landing_page_path: "/products/a",
          utm_source: "google",
          sessions: "12",
          sessions_with_cart_additions: "3",
          sessions_that_reached_checkout: "2",
          sessions_that_completed_checkout: "1",
        },
        { landing_page_path: "/" },
      ],
      DAY,
    );

    const [now, insert, update, del] = brand.pool.calls;
    assert.match(now.sql, /SELECT NOW\(\)/);
    assert.match(insert.sql, /INSERT INTO product_sessions_snapshot/);
    assert.deepEqual(insert.params, [
      DAY, "Product", "/products/a", null, "google", null, null, null, null, null, 12, 3, 2, 1,
      DAY, "Unknown", "/", null, null, null, null, null, null, null, 0, 0, 0, 0,
    ]);
    assert.match(update.sql, /UPDATE product_sessions_snapshot s\s+JOIN product_landing_mapping/);
    assert.deepEqual(update.params, [DAY]);
    assert.match(del.sql, /DELETE FROM product_sessions_snapshot WHERE date = \? AND updated_at < \?/);
    assert.deepEqual(del.params, [DAY, RUN_STARTED]);
    assert.equal(brand.pool.released, 1);
  });
});

describe("refreshMaterializedViews", () => {
  test("upserts the type, path and campaign MVs for the date", async () => {
    const brand = makeBrand();
    await p.refreshMaterializedViews(brand, DAY);
    const [type, path, campaign] = brand.pool.calls;
    assert.match(type.sql, /INSERT INTO mv_product_sessions_by_type_daily/);
    assert.deepEqual(type.params, [DAY]);
    assert.match(path.sql, /INSERT INTO mv_product_sessions_by_path_daily/);
    assert.deepEqual(path.params, [DAY, DAY]);
    assert.match(campaign.sql, /INSERT INTO mv_product_sessions_by_campaign_daily/);
    assert.deepEqual(campaign.params, [DAY, DAY]);
    assert.equal(brand.pool.released, 1);
  });

  test("logs, rethrows and releases on error", async () => {
    const pool = makeFakePool(() => {
      throw new Error("mv fail");
    });
    await assert.rejects(p.refreshMaterializedViews(makeBrand({ pool }), DAY), /mv fail/);
    assert.equal(pool.released, 1);
    assert.match(console.error.mock.calls[0].arguments[0], /MV Refresh Error/);
  });
});

describe("upsertHourlySessionsSummary", () => {
  test("always writes 24 hours, zero-filling gaps and ignoring bad hours", async () => {
    const brand = makeBrand();
    await p.upsertHourlySessionsSummary(
      brand,
      [
        { hour_of_day: "0", sessions: "5", sessions_with_cart_additions: "1" },
        { hour_of_day: 23, sessions: 9 },
        { hour_of_day: 24, sessions: 999 },
        { hour_of_day: "x", sessions: 999 },
      ],
      DAY,
    );
    const { sql, params } = brand.pool.calls[0];
    assert.match(sql, /INSERT INTO hourly_sessions_summary_shopify/);
    assert.equal(params.length, 24 * 4);
    assert.deepEqual(params.slice(0, 4), [DAY, 0, 5, 1]);
    assert.deepEqual(params.slice(4, 8), [DAY, 1, 0, 0]);
    assert.deepEqual(params.slice(-4), [DAY, 23, 9, 0]);
    assert.ok(!params.includes(999));
  });
});

describe("upsertHourlyProductSessions", () => {
  test("does nothing for empty rows", async () => {
    const brand = makeBrand();
    await p.upsertHourlyProductSessions(brand, [], DAY);
    assert.equal(brand.pool.calls.length, 0);
    assert.equal(brand.pool.released, 1);
  });

  test("inserts in chunks of 500, backfills product ids, then sweeps", async () => {
    const brand = makeBrand();
    const rows = Array.from({ length: 1001 }, (_, i) => ({
      hour: i % 24,
      landing_page_path: `/p/${i}`,
      sessions: 1,
    }));
    await p.upsertHourlyProductSessions(brand, rows, DAY);

    const inserts = sqlMatching(brand.pool, /INSERT INTO hourly_product_sessions/);
    assert.deepEqual(
      inserts.map((c) => c.params.length / 12),
      [500, 500, 1],
    );
    assert.deepEqual(inserts[0].params.slice(0, 12), [
      DAY, 0, "Unknown", "/p/0", null, null, null, null, null, null, 1, 0,
    ]);
    assert.equal(sqlMatching(brand.pool, /UPDATE hourly_product_sessions h/).length, 1);
    const [del] = sqlMatching(brand.pool, /DELETE FROM hourly_product_sessions/);
    assert.deepEqual(del.params, [DAY, RUN_STARTED]);
    assert.equal(brand.pool.released, 1);
  });
});

describe("upsertHourlyProductPerformanceRollup", () => {
  test("upserts the rollup for the date and sweeps stale rows", async () => {
    const brand = makeBrand();
    await p.upsertHourlyProductPerformanceRollup(brand, DAY);
    const [now, insert, del] = brand.pool.calls;
    assert.match(now.sql, /SELECT NOW\(\)/);
    assert.match(insert.sql, /INSERT INTO hourly_product_performance_rollup/);
    assert.deepEqual(insert.params, [DAY, DAY, DAY, DAY]);
    assert.match(del.sql, /DELETE FROM hourly_product_performance_rollup/);
    assert.deepEqual(del.params, [DAY, RUN_STARTED]);
    assert.equal(brand.pool.released, 1);
  });
});
