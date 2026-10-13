// Pure / near-pure helpers in pipeline.js: logging, error codes, time, timezone,
// date ranges, concurrency, crypto, brand-config parsing, ShopifyQL builders.
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { setTestEnv, encryptAES, silenceConsole } from "./helpers.js";

const AES_KEY = "unit-test-key";
setTestEnv({ PASSWORD_AES_KEY: AES_KEY, SHOPIFYQL_TIMEZONE: "Asia/Kolkata" });
const p = await import("../pipeline.js");

const brand = { tag: "tst", shopifyQlTimezone: "America/New_York" };
const PAST_DAY = "2024-03-05";

afterEach(() => mock.restoreAll());

describe("runtime log capture", () => {
  test("formatLogArg stringifies strings, errors, objects and circular values", () => {
    assert.equal(p.formatLogArg("hi"), "hi");
    const err = new Error("boom");
    assert.equal(p.formatLogArg(err), err.stack);
    assert.equal(p.formatLogArg({ a: 1 }), '{"a":1}');
    assert.equal(p.formatLogArg(42), "42");
    const circ = {};
    circ.self = circ;
    assert.equal(p.formatLogArg(circ), "[object Object]");
  });

  test("pushRuntimeLog/getLastRuntimeLogs keep only the last 2 lines", () => {
    p.pushRuntimeLog("INFO", ["one"]);
    p.pushRuntimeLog("WARN", ["two", { x: 1 }]);
    p.pushRuntimeLog("ERROR", ["three"]);
    assert.equal(p.getLastRuntimeLogs(), '[WARN] two {"x":1}\n[ERROR] three');
  });
});

describe("error codes", () => {
  test("normalizeErrorCode upper-snakes and falls back", () => {
    assert.equal(p.normalizeErrorCode("foo-bar baz"), "FOO_BAR_BAZ");
    assert.equal(p.normalizeErrorCode("__x__"), "X");
    assert.equal(p.normalizeErrorCode(""), "UNKNOWN_ERROR");
    assert.equal(p.normalizeErrorCode("!!!"), "UNKNOWN_ERROR");
    assert.equal(p.normalizeErrorCode(undefined), "UNKNOWN_ERROR");
  });

  test("deriveErrorCode classifies known failure types", () => {
    assert.equal(p.deriveErrorCode({ message: "Connection timeout" }), "DB_TIMEOUT");
    assert.equal(p.deriveErrorCode({ code: "ER_ACCESS_DENIED_ERROR" }), "DB_AUTH_FAILED");
    assert.equal(p.deriveErrorCode({ message: "Access denied for user" }), "DB_AUTH_FAILED");
    assert.equal(p.deriveErrorCode({ code: "ECONNREFUSED" }), "NETWORK_ERROR");
    assert.equal(p.deriveErrorCode({ code: "ENOTFOUND" }), "NETWORK_ERROR");
    assert.equal(p.deriveErrorCode({ message: "Missing GET_BRANDS_API" }), "ENV_ERROR");
  });

  test("deriveErrorCode falls back to the normalized code/name", () => {
    assert.equal(p.deriveErrorCode(new TypeError("x is not a function")), "TYPEERROR");
    assert.equal(p.deriveErrorCode({ code: "ER_DUP_ENTRY" }), "ER_DUP_ENTRY");
    assert.equal(p.deriveErrorCode(null), "ERROR");
  });
});

describe("IST time helpers", () => {
  // 2025-01-31T20:00:00Z == 2025-02-01T01:30:00 IST
  const NOW = Date.parse("2025-01-31T20:00:00Z");
  const originalTZ = process.env.TZ;

  afterEach(() => {
    mock.timers.reset();
    process.env.TZ = originalTZ;
  });

  for (const tz of ["UTC", "America/Los_Angeles", "Asia/Tokyo"]) {
    test(`are independent of host TZ (${tz})`, () => {
      process.env.TZ = tz;
      mock.timers.enable({ apis: ["Date"], now: NOW });

      const ist = p.nowIST();
      assert.equal(ist.getHours(), 1);
      assert.equal(ist.getMinutes(), 30);
      assert.equal(p.fmtIST(), "2025-02-01T01:30:00+05:30");

      const day = p.todayISTDate();
      assert.deepEqual([day.getFullYear(), day.getMonth(), day.getDate(), day.getHours()], [2025, 1, 1, 0]);
      assert.equal(p.todayISTYMD(), "2025-02-01");
    });
  }

  test("fmtDate formats local date fields as YYYY-MM-DD", () => {
    assert.equal(p.fmtDate(new Date(2024, 0, 9)), "2024-01-09");
    assert.equal(p.fmtDate(new Date(2024, 11, 31)), "2024-12-31");
  });
});

describe("timezone resolution", () => {
  test("isValidTimeZone", () => {
    assert.equal(p.isValidTimeZone("Asia/Kolkata"), true);
    assert.equal(p.isValidTimeZone("UTC"), true);
    assert.equal(p.isValidTimeZone("Mars/Base"), false);
    assert.equal(p.isValidTimeZone(""), false);
    assert.equal(p.isValidTimeZone(null), false);
  });

  test("extractIanaTimeZone accepts plain IANA and Shopify's '(GMT+..) Zone' form", () => {
    assert.equal(p.extractIanaTimeZone("Asia/Kolkata"), "Asia/Kolkata");
    assert.equal(p.extractIanaTimeZone("(GMT+05:30) Asia/Kolkata"), "Asia/Kolkata");
    assert.equal(
      p.extractIanaTimeZone("(GMT-03:00) America/Argentina/Buenos_Aires"),
      "America/Argentina/Buenos_Aires",
    );
    assert.equal(p.extractIanaTimeZone("  Europe/London  "), "Europe/London");
  });

  test("extractIanaTimeZone returns null for empty or unrecognised values", () => {
    assert.equal(p.extractIanaTimeZone(""), null);
    assert.equal(p.extractIanaTimeZone(undefined), null);
    assert.equal(p.extractIanaTimeZone("Pacific Time"), null);
    assert.equal(p.extractIanaTimeZone("(GMT+00:00) Mars/Base"), null);
  });

  test("resolveBrandTimeZone uses parsed zone, else default (warning only if a value was given)", () => {
    const warn = mock.method(console, "warn", () => {});
    assert.equal(p.resolveBrandTimeZone("(GMT-05:00) America/New_York", "B"), "America/New_York");
    assert.equal(p.resolveBrandTimeZone(undefined, "B"), "Asia/Kolkata");
    assert.equal(warn.mock.callCount(), 0);
    assert.equal(p.resolveBrandTimeZone("nonsense", "B"), "Asia/Kolkata");
    assert.equal(warn.mock.callCount(), 1);
    assert.match(warn.mock.calls[0].arguments[0], /invalid store_timezone="nonsense"/);
  });
});

describe("backfill brand selection", () => {
  test("parseBrandIndexList parses, dedupes and validates", () => {
    assert.equal(p.parseBrandIndexList(""), null);
    assert.equal(p.parseBrandIndexList(undefined), null);
    assert.deepEqual([...p.parseBrandIndexList("0, 2,2,,5")], [0, 2, 5]);
    assert.throws(() => p.parseBrandIndexList("1,a"), /Invalid BACKFILL_BRAND_INDICES/);
    assert.throws(() => p.parseBrandIndexList("-1"), /Invalid BACKFILL_BRAND_INDICES/);
  });

  test("selectBackfillBrands returns all brands when no filter is configured", () => {
    const brands = [{ index: 0 }, { index: 1 }];
    assert.equal(p.selectBackfillBrands(brands), brands);
  });
});

describe("date-range helpers", () => {
  test("isValidYMD", () => {
    assert.equal(p.isValidYMD("2024-01-31"), true);
    assert.equal(p.isValidYMD("2024-1-31"), false);
    assert.equal(p.isValidYMD("2024-01-31T00:00:00"), false);
    assert.equal(p.isValidYMD(20240131), false);
  });

  test("ymdToUTCDate / fmtUTCDateToYMD round-trip at UTC midnight", () => {
    const d = p.ymdToUTCDate("2024-02-29");
    assert.equal(d.toISOString(), "2024-02-29T00:00:00.000Z");
    assert.equal(p.fmtUTCDateToYMD(d), "2024-02-29");
  });

  test("addDaysUTC adds whole days", () => {
    const d = p.addDaysUTC(p.ymdToUTCDate("2024-12-31"), 1);
    assert.equal(p.fmtUTCDateToYMD(d), "2025-01-01");
    assert.equal(p.fmtUTCDateToYMD(p.addDaysUTC(d, -2)), "2024-12-30");
  });

  test("buildInclusiveDateRangeYMD is inclusive and crosses month/leap boundaries", () => {
    assert.deepEqual(p.buildInclusiveDateRangeYMD("2024-02-28", "2024-03-01"), [
      "2024-02-28",
      "2024-02-29",
      "2024-03-01",
    ]);
    assert.deepEqual(p.buildInclusiveDateRangeYMD("2024-05-05", "2024-05-05"), ["2024-05-05"]);
  });

  test("buildInclusiveDateRangeYMD rejects bad formats and reversed ranges", () => {
    assert.throws(() => p.buildInclusiveDateRangeYMD("2024-5-1", "2024-05-02"), /Invalid date format/);
    assert.throws(() => p.buildInclusiveDateRangeYMD("", "2024-05-02"), /Invalid date format/);
    assert.throws(() => p.buildInclusiveDateRangeYMD("2024-05-03", "2024-05-02"), /Start date is after end date/);
  });

  test("shiftYMD shifts labels across year boundaries", () => {
    assert.equal(p.shiftYMD("2025-01-01", -1), "2024-12-31");
    assert.equal(p.shiftYMD("2024-02-28", 1), "2024-02-29");
    assert.equal(p.shiftYMD("2024-02-28", 0), "2024-02-28");
  });

  test("recentDatesForTimeZone returns the last N brand-local days ending today", () => {
    // 2025-03-01T03:00Z: already Mar 1 in Kolkata, still Feb 28 in Los Angeles.
    mock.timers.enable({ apis: ["Date"], now: Date.parse("2025-03-01T03:00:00Z") });
    try {
      assert.deepEqual(p.recentDatesForTimeZone("Asia/Kolkata", 2), ["2025-02-28", "2025-03-01"]);
      assert.deepEqual(p.recentDatesForTimeZone("America/Los_Angeles", 3), [
        "2025-02-26",
        "2025-02-27",
        "2025-02-28",
      ]);
      assert.deepEqual(p.recentDatesForTimeZone("UTC", 1), ["2025-03-01"]);
    } finally {
      mock.timers.reset();
    }
  });
});

describe("mapWithConcurrency", () => {
  test("preserves input order, passes index, and caps in-flight work", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const results = await p.mapWithConcurrency([30, 10, 20, 5, 1], 2, async (ms, idx) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((r) => setTimeout(r, ms));
      inFlight -= 1;
      return `${idx}:${ms}`;
    });
    assert.deepEqual(results, ["0:30", "1:10", "2:20", "3:5", "4:1"]);
    assert.equal(maxInFlight, 2);
  });

  test("handles empty input and concurrency larger than input", async () => {
    assert.deepEqual(await p.mapWithConcurrency([], 3, async () => 1), []);
    assert.deepEqual(await p.mapWithConcurrency([1, 2], 10, async (x) => x * 2), [2, 4]);
  });

  test("propagates mapper rejection", async () => {
    await assert.rejects(
      p.mapWithConcurrency([1, 2], 1, async (x) => {
        if (x === 2) throw new Error("bad item");
      }),
      /bad item/,
    );
  });
});

describe("AES helpers", () => {
  test("normalizeKey zero-pads short keys and truncates long ones to 32 bytes", () => {
    const short = p.normalizeKey("abc");
    assert.equal(short.length, 32);
    assert.equal(short.subarray(0, 3).toString(), "abc");
    assert.ok(short.subarray(3).every((b) => b === 0));

    const long = p.normalizeKey("x".repeat(40));
    assert.equal(long.length, 32);
    assert.equal(p.normalizeKey("y".repeat(32)).toString(), "y".repeat(32));
  });

  test("decryptAES passes through falsy and colon-less values", () => {
    assert.equal(p.decryptAES(""), "");
    assert.equal(p.decryptAES(undefined), undefined);
    assert.equal(p.decryptAES("plain-token"), "plain-token");
  });

  test("decryptAES decrypts iv:ciphertext produced with PASSWORD_AES_KEY", () => {
    assert.equal(p.decryptAES(encryptAES("shpat_secret", AES_KEY)), "shpat_secret");
  });

  test("decryptAES throws (and logs) on a wrong key / corrupt ciphertext", () => {
    silenceConsole();
    assert.throws(() => p.decryptAES(encryptAES("x", "some-other-key")));
    assert.match(console.error.mock.calls[0].arguments[0], /DECRYPT ERROR/);
  });

  test("decryptAESMaybe passes through and decrypts when a key is set", () => {
    assert.equal(p.decryptAESMaybe(null), null);
    assert.equal(p.decryptAESMaybe("plain"), "plain");
    assert.equal(p.decryptAESMaybe(encryptAES("pw", AES_KEY)), "pw");
  });
});

describe("brand-config parsing", () => {
  test("pickField returns the first non-null key", () => {
    const obj = { a: null, b: undefined, c: 0, d: "x" };
    assert.equal(p.pickField(obj, ["a", "b", "c", "d"]), 0);
    assert.equal(p.pickField(obj, ["d"]), "x");
    assert.equal(p.pickField(obj, ["a", "zzz"]), undefined);
  });

  test("parseCsv trims and drops blanks", () => {
    assert.deepEqual(p.parseCsv(" a, b ,,c "), ["a", "b", "c"]);
    assert.deepEqual(p.parseCsv(undefined), []);
    assert.deepEqual(p.parseCsv(""), []);
  });

  test("buildBrandListFromIndexedEnv collects indexed brands in index order", () => {
    const list = p.buildBrandListFromIndexedEnv({
      API_VERSION: "2025-01",
      SHOP_NAME_2: "two",
      DB_HOST_2: "h2",
      BRAND_ID_0: "77",
      SHOP_NAME_0: "zero",
      API_VERSION_0: "2025-10",
      ACCESS_TOKEN_0: "t0",
      DB_HOST_0: "h0",
      DB_USER_0: "u0",
      DB_PASSWORD_0: "p0",
      DB_DATABASE_0: "d0",
      BRAND_TAG_0: "z",
      BRAND_NAME_0: "Zero",
      STORE_TIMEZONE_0: "UTC",
    });
    assert.equal(list.length, 2);
    assert.deepEqual(list[0], {
      id: "77",
      shop_name: "zero",
      api_version: "2025-10",
      access_token: "t0",
      db_host: "h0",
      db_user: "u0",
      db_password: "p0",
      db_database: "d0",
      brand_tag: "z",
      brand_name: "Zero",
      store_timezone: "UTC",
    });
    assert.equal(list[1].shop_name, "two");
    assert.equal(list[1].api_version, "2025-01", "falls back to global API_VERSION");
    assert.equal(list[1].id, 3, "falls back to index + 1");
  });

  test("buildBrandListFromIndexedEnv uses ACTIVE_BRAND_IDS for ids and indexes", () => {
    const list = p.buildBrandListFromIndexedEnv({ ACTIVE_BRAND_IDS: "10, 20" });
    assert.deepEqual(
      list.map((b) => b.id),
      ["10", "20"],
    );
  });

  test("buildBrandListFromIndexedEnv returns [] when nothing is configured", () => {
    assert.deepEqual(p.buildBrandListFromIndexedEnv({ FOO: "bar" }), []);
  });
});

describe("ShopifyQL query builders", () => {
  test("buildShopifyQLQuery: single-line, timezone, explicit day, default LIMIT, no OFFSET", () => {
    const q = p.buildShopifyQLQuery(brand, PAST_DAY);
    assert.ok(!q.includes("\n"));
    assert.match(q, /FROM sessions/);
    assert.match(q, /WITH TIMEZONE 'America\/New_York'/);
    assert.match(q, /SINCE 2024-03-05T00:00:00 UNTIL 2024-03-05T23:59:59/);
    assert.match(q, /LIMIT 1000\s/);
    assert.doesNotMatch(q, /OFFSET/);
    assert.match(q, /ORDER BY sessions DESC,\s+landing_page_type ASC/);
  });

  test("buildShopifyQLQuery: custom limit/offset and DURING today for null date", () => {
    const q = p.buildShopifyQLQuery(brand, null, { limit: 50, offset: 100 });
    assert.match(q, /LIMIT 50 OFFSET 100/);
    assert.match(q, /DURING today/);
  });

  test("buildShopifyQLHourlyQuery groups by hour_of_day", () => {
    const q = p.buildShopifyQLHourlyQuery(brand, PAST_DAY);
    assert.ok(!q.includes("\n"));
    assert.match(q, /GROUP BY hour_of_day/);
    assert.match(q, /WITH TIMEZONE 'America\/New_York'/);
    assert.match(q, /SINCE 2024-03-05T00:00:00/);
  });

  test("buildHourClause zero-pads the hour", () => {
    assert.equal(p.buildHourClause(PAST_DAY, 5), "SINCE 2024-03-05T05:00:00 UNTIL 2024-03-05T05:59:59");
    assert.equal(p.buildHourClause(PAST_DAY, 23), "SINCE 2024-03-05T23:00:00 UNTIL 2024-03-05T23:59:59");
  });

  test("buildShopifyQLHourlyDimensionAllHoursQuery includes hour_of_day + all dims", () => {
    const q = p.buildShopifyQLHourlyDimensionAllHoursQuery(brand, PAST_DAY);
    assert.ok(!q.includes("\n"));
    assert.match(q, /GROUP BY\s+hour_of_day,\s+landing_page_type/);
    assert.match(q, /referrer_name/);
    assert.match(q, /SINCE 2024-03-05T00:00:00 UNTIL 2024-03-05T23:59:59/);
  });

  test("buildShopifyQLHourlyDimensionQuery uses a one-hour window", () => {
    const q = p.buildShopifyQLHourlyDimensionQuery(brand, PAST_DAY, 7);
    assert.ok(!q.includes("\n"));
    assert.match(q, /SINCE 2024-03-05T07:00:00 UNTIL 2024-03-05T07:59:59/);
    assert.doesNotMatch(q, /hour_of_day/);
  });
});
