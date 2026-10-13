// backfill_product_type_funnel.js run() with BACKFILL_LOOKBACK_DAYS (brand-local window).
import { test, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import {
  setTestEnv,
  BRANDS_API,
  apiBrand,
  mockBrandsApi,
  mockShopifyQL,
  fastTimers,
  silenceConsole,
} from "./helpers.js";

setTestEnv({
  GET_BRANDS_API: BRANDS_API,
  PIPELINE_AUTH_HEADER: "hdr",
  PASSWORD_AES_KEY: "k",
  BACKFILL_LOOKBACK_DAYS: "2",
  // A fixed range is also set: the lookback window must win.
  BACKFILL_START_IST_DATE: "2020-01-01",
  BACKFILL_END_IST_DATE: "2020-01-05",
});
delete process.env.BACKFILL_BRAND_INDICES;
const f = await import("../backfill_product_type_funnel.js");

afterEach(() => {
  mock.restoreAll();
  mock.timers.reset();
});

test("run refetches each brand's last N local days, ignoring START/END", async () => {
  silenceConsole();
  fastTimers();
  mock.timers.enable({ apis: ["Date"], now: Date.parse("2025-03-01T03:00:00Z") });
  mockBrandsApi(axios, mysql, {
    1: apiBrand(1),
    2: apiBrand(2, { store_timezone: "America/Los_Angeles" }),
  });
  const post = mockShopifyQL(axios);

  await f.run();

  const queries = post.mock.calls.map((c) => c.arguments[1].query);
  assert.ok(!queries.some((q) => q.includes("2020-01")));
  // Past days use an explicit SINCE; each brand's "today" uses DURING today.
  assert.ok(queries.some((q) => q.includes("SINCE 2025-02-28") && q.includes("Asia/Kolkata")));
  assert.ok(queries.some((q) => q.includes("SINCE 2025-02-27") && q.includes("America/Los_Angeles")));
  assert.equal(queries.filter((q) => q.includes("DURING today")).length, 2);
  assert.equal(queries.length, 4);
});
