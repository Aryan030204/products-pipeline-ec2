// Product-type sales/orders worker.
//
// Fetches Shopify's ShopifyQL `sales` report grouped by (product_type, day),
// stores it in a raw staging table, then rolls it up together with the
// existing per-product-type session data (mv_product_sessions_by_type_daily,
// refreshed earlier in the same pipeline.js run) into one funnel table:
//   date, product_type, total_orders, gross_revenue, net_revenue,
//   sessions, sessions_with_cart_additions, conversion_rate
//
// Called from pipeline.js's processBrand() for every (brand, date) — both the
// normal hourly run and the rolling/retrospective backfill.

import axios from "axios";
import {
  sleep,
  formatShopifyQLTable,
  buildDayClause,
  ShopifyQLFetchError,
  getThrottleResetSleepMsFromGraphQLErrors,
} from "./shopifyql_utils.js";

const SHOPIFYQL_FETCH_MAX_RETRIES = Math.max(
  1,
  parseInt(process.env.SHOPIFYQL_FETCH_MAX_RETRIES || "5", 10) || 5,
);

// ---------- DB setup ----------
export async function ensureProductTypeSalesTables(brand) {
  if (brand._productTypeSalesTablesEnsured) return;

  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_type_sales_snapshot (
        date DATE NOT NULL,
        product_type VARCHAR(255) NOT NULL DEFAULT 'Unknown',
        orders INT NOT NULL DEFAULT 0,
        gross_sales DECIMAL(14,2) NOT NULL DEFAULT 0,
        net_sales DECIMAL(14,2) NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, product_type)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_type_funnel_daily (
        date DATE NOT NULL,
        product_type VARCHAR(255) NOT NULL DEFAULT 'Unknown',
        total_orders INT NOT NULL DEFAULT 0,
        gross_revenue DECIMAL(14,2) NOT NULL DEFAULT 0,
        net_revenue DECIMAL(14,2) NOT NULL DEFAULT 0,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        conversion_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, product_type)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    brand._productTypeSalesTablesEnsured = true;
  } finally {
    conn.release();
  }
}

// ---------- Fetch ----------
function buildProductTypeSalesQuery(brand, targetYmd) {
  const dayClause = buildDayClause(targetYmd, brand.shopifyQlTimezone);
  // ShopifyQL only allows a single WITH clause per query — GROUP_TOTALS/TOTALS
  // and TIMEZONE must be combined into one, not issued as two separate WITHs.
  const withClause = `WITH GROUP_TOTALS, TOTALS, TIMEZONE '${brand.shopifyQlTimezone}'`;

  return `
    FROM sales
      SHOW net_sales, gross_sales, orders
      GROUP BY product_type, day
      ${withClause}
      ${dayClause}
      ORDER BY net_sales__product_type_totals DESC, day ASC, net_sales DESC,
        product_type ASC
      LIMIT 1000
    VISUALIZE net_sales TYPE table
  `.replace(/\n+/g, " ");
}

// GROUP_TOTALS/TOTALS rows come back alongside the real per-(product_type,day)
// rows. Since the request is already scoped to a single day, a real row has
// both a non-empty product_type AND a day matching targetYmd; the summary
// rows are missing one or both of those fields. Keep only real rows.
function filterRealDayRows(rows, targetYmd) {
  const real = [];
  let discarded = 0;

  for (const r of rows) {
    const productType = r.product_type;
    const day = r.day;
    const hasProductType = typeof productType === "string" && productType.trim() !== "";
    const matchesDay = typeof day === "string" && day.slice(0, 10) === targetYmd;

    if (hasProductType && matchesDay) {
      real.push(r);
    } else {
      discarded++;
    }
  }

  if (discarded > 0) {
    console.log(
      `[product_type_sales] Discarded ${discarded} GROUP_TOTALS/TOTALS row(s) for ${targetYmd}.`,
    );
  }

  return real;
}

export async function fetchProductTypeSales(brand, targetYmd) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/graphql.json`;
  const q = buildProductTypeSalesQuery(brand, targetYmd).replace(/"/g, '\\"');
  const graphql = {
    query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
  };

  const MAX_RETRIES = SHOPIFYQL_FETCH_MAX_RETRIES;
  let attempt = 1;

  while (attempt <= MAX_RETRIES) {
    const resp = await axios.post(url, graphql, {
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": brand.accessToken,
      },
      timeout: 60000,
      validateStatus: () => true,
    });

    if (resp.status === 429) {
      const retry = Number(resp.headers["retry-after"] || "3");
      console.log(
        `[${brand.tag}] ShopifyQL sales rate-limited (429), sleeping ${retry}s (attempt ${attempt}/${MAX_RETRIES})`,
      );
      await sleep(retry * 1000);
      continue; // 429 is not a wasted attempt
    }

    if (resp.status >= 500) {
      const sleepMs = Math.min(5000, 500 * attempt);
      console.warn(
        `[${brand.tag}] ShopifyQL sales got ${resp.status}, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
      );
      await sleep(sleepMs);
      attempt += 1;
      continue;
    }

    if (resp.status !== 200) {
      const sleepMs = Math.min(5000, 500 * attempt);
      console.error(
        `[${brand.tag}] ShopifyQL sales failed: HTTP ${resp.status} (attempt ${attempt}/${MAX_RETRIES})`,
      );
      await sleep(sleepMs);
      attempt += 1;
      continue;
    }

    if (resp.data?.errors?.length) {
      const throttleSleepMs = getThrottleResetSleepMsFromGraphQLErrors(resp.data.errors);
      if (throttleSleepMs != null) {
        console.warn(
          `[${brand.tag}] ShopifyQL sales THROTTLED, sleeping ${Math.ceil(throttleSleepMs / 1000)}s until window reset (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(throttleSleepMs);
        continue; // Shopify told us exactly when to retry; don't burn an attempt
      }

      const sleepMs = Math.min(5000, 500 * attempt);
      console.warn(
        `[${brand.tag}] ShopifyQL sales GraphQL errors (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(resp.data.errors)}`,
      );
      await sleep(sleepMs);
      attempt += 1;
      continue;
    }

    const res = resp.data.data?.shopifyqlQuery;
    if (!res) {
      const sleepMs = Math.min(5000, 500 * attempt);
      console.warn(
        `[${brand.tag}] ShopifyQL sales missing shopifyqlQuery payload (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
      );
      await sleep(sleepMs);
      attempt += 1;
      continue;
    }

    if (res.parseErrors?.length) {
      const sleepMs = Math.min(5000, 500 * attempt);
      console.warn(
        `[${brand.tag}] ShopifyQL sales parseErrors, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(res.parseErrors)}`,
      );
      await sleep(sleepMs);
      attempt += 1;
      continue;
    }

    const rows = formatShopifyQLTable(res.tableData);
    console.log(
      `[${brand.tag}] ShopifyQL sales fetched ${rows.length} raw row(s) for ${targetYmd}.`,
    );
    return filterRealDayRows(rows, targetYmd);
  }

  throw new ShopifyQLFetchError(
    `[${brand.tag}] ShopifyQL sales-by-product-type failed after ${MAX_RETRIES} attempts for ${targetYmd}.`,
  );
}

// ---------- Raw staging upsert ----------
export async function upsertProductTypeSalesSnapshot(brand, rows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    if (!rows.length) {
      console.log(`[${brand.tag}] No product-type sales rows for ${targetYmd}.`);
      return;
    }

    const insertRows = rows.map((r) => [
      targetYmd,
      (r.product_type && String(r.product_type).trim()) || "Unknown",
      Number(r.orders || 0),
      Number(r.gross_sales || 0),
      Number(r.net_sales || 0),
    ]);

    const placeholders = insertRows.map(() => "(?, ?, ?, ?, ?)").join(", ");

    await conn.query(
      `
      INSERT INTO product_type_sales_snapshot
        (date, product_type, orders, gross_sales, net_sales)
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE
        orders       = VALUES(orders),
        gross_sales  = VALUES(gross_sales),
        net_sales    = VALUES(net_sales),
        updated_at   = CURRENT_TIMESTAMP
    `,
      insertRows.flat(),
    );
  } finally {
    conn.release();
  }
}

// ---------- Funnel rollup ----------
// MySQL has no FULL OUTER JOIN, so this is two upserts instead of one query:
// the first seeds/updates every product_type that has session data that day,
// the second seeds/updates every product_type that has sales data that day.
// Together they cover a product_type appearing in only one side without
// either INNER-JOIN dropping it or a manual UNION/DISTINCT trick.
export async function refreshProductTypeFunnelRollup(brand, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    // ---------- Seed/update from session data ----------
    await conn.query(
      `
      INSERT INTO mv_product_type_funnel_daily
        (date, product_type, sessions, sessions_with_cart_additions, conversion_rate)
      SELECT
        date,
        product_type,
        SUM(sessions) AS sessions,
        SUM(sessions_with_cart_additions) AS sessions_with_cart_additions,
        0 AS conversion_rate
      FROM mv_product_sessions_by_type_daily
      WHERE date = ?
      GROUP BY date, product_type
      ON DUPLICATE KEY UPDATE
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        updated_at                   = CURRENT_TIMESTAMP
    `,
      [targetYmd],
    );

    // ---------- Seed/update from sales data + recompute conversion_rate ----------
    await conn.query(
      `
      INSERT INTO mv_product_type_funnel_daily
        (date, product_type, total_orders, gross_revenue, net_revenue, conversion_rate)
      SELECT
        date,
        product_type,
        orders AS total_orders,
        gross_sales AS gross_revenue,
        net_sales AS net_revenue,
        0 AS conversion_rate
      FROM product_type_sales_snapshot
      WHERE date = ?
      ON DUPLICATE KEY UPDATE
        total_orders   = VALUES(total_orders),
        gross_revenue  = VALUES(gross_revenue),
        net_revenue    = VALUES(net_revenue),
        updated_at     = CURRENT_TIMESTAMP
    `,
      [targetYmd],
    );

    // ---------- Recompute conversion_rate now that both sides are in ----------
    await conn.query(
      `
      UPDATE mv_product_type_funnel_daily
      SET conversion_rate = CASE
        WHEN sessions > 0 THEN ROUND(total_orders / sessions, 4)
        ELSE 0
      END
      WHERE date = ?
    `,
      [targetYmd],
    );

    console.log(`[${brand.tag}] Refreshed product-type funnel rollup for ${targetYmd}.`);
  } finally {
    conn.release();
  }
}
