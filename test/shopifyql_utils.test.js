import { test, describe, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import {
  getDatePartsInTimeZone,
  todayYMDInTimeZone,
  sleep,
  formatShopifyQLTable,
  buildDayClause,
  ShopifyQLFetchError,
  getThrottleResetSleepMsFromGraphQLErrors,
} from "../shopifyql_utils.js";

afterEach(() => mock.restoreAll());

describe("getDatePartsInTimeZone", () => {
  test("returns zero-padded parts in the given timezone", () => {
    // 2025-01-31T20:00Z is already Feb 1 in Kolkata (+05:30), still Jan 31 in New York.
    const d = new Date("2025-01-31T20:00:00Z");
    assert.deepEqual(getDatePartsInTimeZone("Asia/Kolkata", d), {
      year: "2025",
      month: "02",
      day: "01",
    });
    assert.deepEqual(getDatePartsInTimeZone("America/New_York", d), {
      year: "2025",
      month: "01",
      day: "31",
    });
  });

  test("throws on an invalid timezone", () => {
    assert.throws(() => getDatePartsInTimeZone("Not/AZone"), RangeError);
  });
});

describe("todayYMDInTimeZone", () => {
  test("formats today as YYYY-MM-DD", () => {
    const { year, month, day } = getDatePartsInTimeZone("UTC");
    assert.equal(todayYMDInTimeZone("UTC"), `${year}-${month}-${day}`);
    assert.match(todayYMDInTimeZone("Asia/Kolkata"), /^\d{4}-\d{2}-\d{2}$/);
  });
});

describe("sleep", () => {
  test("resolves after the requested delay via setTimeout", async () => {
    const delays = [];
    const original = globalThis.setTimeout;
    mock.method(globalThis, "setTimeout", (fn, ms) => {
      delays.push(ms);
      return original(fn, 0);
    });
    const result = await sleep(1234);
    assert.equal(result, undefined);
    assert.deepEqual(delays, [1234]);
  });
});

describe("formatShopifyQLTable", () => {
  test("copies object-style rows", () => {
    const row = { a: 1, b: "x" };
    const out = formatShopifyQLTable({ rows: [row] });
    assert.deepEqual(out, [{ a: 1, b: "x" }]);
    assert.notEqual(out[0], row, "should be a shallow copy");
  });

  test("maps legacy array rows onto column names", () => {
    const out = formatShopifyQLTable({
      columns: [{ name: "hour_of_day" }, { name: "sessions" }],
      rows: [
        [0, 10],
        [1, 20, "extra-ignored"],
      ],
    });
    assert.deepEqual(out, [
      { hour_of_day: 0, sessions: 10 },
      { hour_of_day: 1, sessions: 20 },
    ]);
  });

  test("skips null/primitive rows and tolerates missing rows/columns", () => {
    assert.deepEqual(formatShopifyQLTable({ rows: [null, 5, "s"] }), []);
    assert.deepEqual(formatShopifyQLTable({}), []);
  });
});

describe("buildDayClause", () => {
  test("uses DURING today for null or today's date", () => {
    assert.equal(buildDayClause(null, "UTC"), "DURING today");
    assert.equal(buildDayClause(todayYMDInTimeZone("UTC"), "UTC"), "DURING today");
  });

  test("uses an explicit full-day SINCE/UNTIL range for other dates", () => {
    assert.equal(
      buildDayClause("2024-03-05", "UTC"),
      "SINCE 2024-03-05T00:00:00 UNTIL 2024-03-05T23:59:59",
    );
  });
});

describe("ShopifyQLFetchError", () => {
  test("is an Error with its own name", () => {
    const err = new ShopifyQLFetchError("boom");
    assert.ok(err instanceof Error);
    assert.equal(err.name, "ShopifyQLFetchError");
    assert.equal(err.message, "boom");
  });
});

describe("getThrottleResetSleepMsFromGraphQLErrors", () => {
  test("returns null for empty / non-array / non-throttle errors", () => {
    assert.equal(getThrottleResetSleepMsFromGraphQLErrors(undefined), null);
    assert.equal(getThrottleResetSleepMsFromGraphQLErrors([]), null);
    assert.equal(
      getThrottleResetSleepMsFromGraphQLErrors([{ message: "x", extensions: { code: "OTHER" } }]),
      null,
    );
  });

  test("returns null when windowResetAt is missing or unparseable", () => {
    assert.equal(
      getThrottleResetSleepMsFromGraphQLErrors([{ extensions: { code: "THROTTLED", cost: {} } }]),
      null,
    );
    assert.equal(
      getThrottleResetSleepMsFromGraphQLErrors([
        { extensions: { code: "THROTTLED", cost: { windowResetAt: "garbage" } } },
      ]),
      null,
    );
  });

  test("returns ms until reset plus a 250ms buffer", () => {
    mock.method(Date, "now", () => Date.parse("2025-01-01T00:00:00Z"));
    const ms = getThrottleResetSleepMsFromGraphQLErrors([
      { extensions: { code: "THROTTLED", cost: { windowResetAt: "2025-01-01T00:00:02Z" } } },
    ]);
    assert.equal(ms, 2250);
  });

  test("clamps a reset time in the past to just the buffer", () => {
    mock.method(Date, "now", () => Date.parse("2025-01-01T00:00:10Z"));
    const ms = getThrottleResetSleepMsFromGraphQLErrors([
      { extensions: { code: "THROTTLED", cost: { windowResetAt: "2025-01-01T00:00:00Z" } } },
    ]);
    assert.equal(ms, 250);
  });
});
