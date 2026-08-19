// Shared ShopifyQL helpers used by both pipeline.js and product_type_sales.js.
// Extracted verbatim from pipeline.js so both callers share one retry/parsing
// implementation instead of drifting copies (as hourly_backfill.js had).

export function getDatePartsInTimeZone(timeZone, date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));

  return {
    year: values.year,
    month: values.month,
    day: values.day,
  };
}

export function todayYMDInTimeZone(timeZone) {
  const { year, month, day } = getDatePartsInTimeZone(timeZone);
  return `${year}-${month}-${day}`;
}

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function formatShopifyQLTable(tableData) {
  const columns = tableData.columns || [];
  const rows = tableData.rows || [];
  const out = [];

  for (const row of rows) {
    // NEW STYLE (object rows)
    if (row && typeof row === "object" && !Array.isArray(row)) {
      out.push({ ...row });
      continue;
    }

    // LEGACY STYLE (array rows)
    if (Array.isArray(row)) {
      const obj = {};
      row.forEach((val, idx) => {
        if (columns[idx]) obj[columns[idx].name] = val;
      });
      out.push(obj);
    }
  }

  return out;
}

export function buildDayClause(targetYmd, timeZone) {
  // Avoid "future UNTIL" for today; use DURING today for current day runs.
  if (!targetYmd || targetYmd === todayYMDInTimeZone(timeZone)) {
    return `DURING today`;
  }
  return `SINCE ${targetYmd}T00:00:00 UNTIL ${targetYmd}T23:59:59`;
}

// Thrown when a ShopifyQL fetch can't be completed (a page/request exhausted
// its retries, or pagination ran past the page cap). Callers catch this to
// skip the affected work instead of persisting a partial/truncated result.
export class ShopifyQLFetchError extends Error {
  constructor(message) {
    super(message);
    this.name = "ShopifyQLFetchError";
  }
}

export function getThrottleResetSleepMsFromGraphQLErrors(errors) {
  if (!Array.isArray(errors) || !errors.length) return null;

  const throttled = errors.find(
    (e) => e?.extensions?.code === "THROTTLED" && e?.extensions?.cost?.windowResetAt,
  );
  if (!throttled) return null;

  const resetAt = Date.parse(throttled.extensions.cost.windowResetAt);
  if (!Number.isFinite(resetAt)) return null;

  // Add a small buffer so we don't wake up exactly on the boundary.
  return Math.max(0, resetAt - Date.now()) + 250;
}
