/**
 * Minimal Hourly Sessions Backfill (ShopifyQL -> hourly_sessions_summary_shopify)
 *
 * Env:
 *   TOTAL_CONFIG_COUNT=...
 *   SHOP_NAME_0=... API_VERSION_0=... ACCESS_TOKEN_0=...
 *   DB_HOST_0=... DB_USER_0=... DB_PASSWORD_0=... DB_DATABASE_0=...
 *   ... repeat for brands ...
 *
 * Backfill range:
 *   HOURLY_BACKFILL_MODE=true
 *   HOURLY_BACKFILL_START_IST_DATE=2025-10-01
 *   HOURLY_BACKFILL_END_IST_DATE=2025-12-14
 *
 * Optional:
 *   SHOPIFYQL_TIMEZONE=Asia/Kolkata
 */

import "dotenv/config";
import mysql from "mysql2/promise";
import axios from "axios";
import fs from "fs";

// ---------- env ----------
const MODE = String(process.env.HOURLY_BACKFILL_MODE || "").toLowerCase().trim() === "true";
const START = process.env.HOURLY_BACKFILL_START_IST_DATE;
const END = process.env.HOURLY_BACKFILL_END_IST_DATE;
const SHOPIFYQL_TIMEZONE = process.env.SHOPIFYQL_TIMEZONE || "Asia/Kolkata";

// ---------- date helpers (UTC-safe iteration for YYYY-MM-DD lists) ----------
function isValidYMD(s) {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s);
}
function ymdToUTCDate(ymd) {
  const [y, m, d] = ymd.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}
function fmtUTCDateToYMD(dt) {
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dt.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
function addDaysUTC(dt, days) {
  return new Date(dt.getTime() + days * 86400000);
}
function buildInclusiveDateRangeYMD(startYmd, endYmd) {
  if (!isValidYMD(startYmd) || !isValidYMD(endYmd)) {
    throw new Error(`[HOURLY] Invalid date format. Use YYYY-MM-DD. start=${startYmd}, end=${endYmd}`);
  }
  const start = ymdToUTCDate(startYmd);
  const end = ymdToUTCDate(endYmd);
  if (start.getTime() > end.getTime()) throw new Error(`[HOURLY] Start > end: ${startYmd} > ${endYmd}`);

  const out = [];
  for (let cur = start; cur.getTime() <= end.getTime(); cur = addDaysUTC(cur, 1)) {
    out.push(fmtUTCDateToYMD(cur));
  }
  return out;
}

// ---------- brand config ----------
function loadBrands() {
  const count = parseInt(process.env.TOTAL_CONFIG_COUNT || "0", 10);
  const brands = [];

  for (let i = 0; i < count; i++) {
    const shopName = process.env[`SHOP_NAME_${i}`];
    const apiVersion = process.env[`API_VERSION_${i}`];
    const accessToken = process.env[`ACCESS_TOKEN_${i}`];

    const dbHost = process.env[`DB_HOST_${i}`];
    const dbUser = process.env[`DB_USER_${i}`];
    const dbPassword = process.env[`DB_PASSWORD_${i}`];
    const dbDatabase = process.env[`DB_DATABASE_${i}`];
    const dbSslEnabledEnv = String(
      process.env[`DB_SSL_ENABLED_${i}`] || process.env.DB_SSL_ENABLED || ""
    ).toLowerCase();
    let dbSslEnabled = dbSslEnabledEnv === "true";

    // Default to TLS for RDS/Proxy hosts unless explicitly disabled
    if (!dbSslEnabled && dbHost && dbSslEnabledEnv === "" && /rds\.amazonaws\.com$/i.test(dbHost)) {
      dbSslEnabled = true;
    }

    const dbSslCa = process.env[`DB_SSL_CA_${i}`] || process.env.DB_SSL_CA;
    const dbSslCaFile = process.env[`DB_SSL_CA_FILE_${i}`] || process.env.DB_SSL_CA_FILE;

    let ssl;
    if (dbSslEnabled || dbSslCa || dbSslCaFile) {
      let ca = dbSslCa ? dbSslCa.replace(/\\n/g, "\n") : undefined;
      if (!ca && dbSslCaFile) {
        try {
          ca = fs.readFileSync(dbSslCaFile, "utf8");
        } catch (err) {
          console.warn(
            `[INIT] Failed to read CA file for brand ${i} (${dbSslCaFile}): ${err?.message}`
          );
        }
      }

      ssl = {
        // Match "require: true, rejectUnauthorized: false" semantics used in some clients
        rejectUnauthorized: false,
        ...(ca ? { ca } : {}),
      };
    }

    const brandTag = process.env[`BRAND_TAG_${i}`] || `brand_${i}`;
    const brandName = process.env[`BRAND_NAME_${i}`] || brandTag.toUpperCase();

    if (!shopName || !apiVersion || !accessToken || !dbHost || !dbUser || !dbDatabase) {
      console.warn(`[INIT] Skipping brand index ${i} – missing env(s).`);
      continue;
    }

    const pool = mysql.createPool({
      host: dbHost,
      user: dbUser,
      password: dbPassword,
      database: dbDatabase,
      waitForConnections: true,
      connectionLimit: 5,
      queueLimit: 0,
      ...(ssl ? { ssl } : {}),
    });

    brands.push({
      index: i,
      tag: brandTag,
      name: brandName,
      shopName,
      apiVersion,
      accessToken,
      pool,
    });
  }

  console.log(`[INIT] Active brands: ${brands.map((b) => `${b.index}:${b.name}`).join(", ")}`);
  return brands;
}

// ---------- ensure hourly table ----------
async function ensureHourlyTable(brand) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify (
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        number_of_sessions INT DEFAULT 0,
        number_of_atc_sessions INT DEFAULT 0,
        adjusted_number_of_sessions INT NULL,
        PRIMARY KEY (date, hour),
        KEY idx_date (date)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);
  } finally {
    conn.release();
  }
}

// ---------- ShopifyQL hourly ----------
function buildShopifyQLHourlyQuery(targetYmd) {
  return `
    FROM sessions
      SHOW hour_of_day, sessions, sessions_with_cart_additions
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY hour_of_day
      WITH TIMEZONE '${SHOPIFYQL_TIMEZONE}'
      SINCE ${targetYmd}T00:00:00 UNTIL ${targetYmd}T23:59:59
      ORDER BY hour_of_day ASC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

function formatShopifyQLTable(tableData) {
  const columns = tableData.columns || [];
  const rows = tableData.rows || [];
  const out = [];

  for (const row of rows) {
    if (row && typeof row === "object" && !Array.isArray(row)) {
      out.push({ ...row });
      continue;
    }
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

async function fetchShopifyQLHourlySessions(brand, targetYmd) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/2025-10/graphql.json`;
  const q = buildShopifyQLHourlyQuery(targetYmd).replace(/"/g, '\\"');

  const graphql = {
    query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
  };

  while (true) {
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
      console.log(`[${brand.tag}] Rate-limited, sleeping ${retry}s`);
      await new Promise((r) => setTimeout(r, retry * 1000));
      continue;
    }

    if (resp.status !== 200 || resp.data.errors) return [];

    const res = resp.data.data?.shopifyqlQuery;
    if (!res || res.parseErrors?.length) return [];

    return formatShopifyQLTable(res.tableData);
  }
}

// ---------- MySQL upsert (always writes 24 rows/day) ----------
async function upsertHourlySessionsSummary(brand, hourlyRows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`DELETE FROM hourly_sessions_summary_shopify WHERE date = ?`, [targetYmd]);

    const byHour = new Map();
    for (const r of hourlyRows) {
      const h = Number(r.hour_of_day);
      if (!Number.isFinite(h) || h < 0 || h > 23) continue;
      byHour.set(h, {
        sessions: Number(r.sessions || 0),
        atc: Number(r.sessions_with_cart_additions || 0),
      });
    }

    const rows = [];
    for (let h = 0; h < 24; h++) {
      const v = byHour.get(h) || { sessions: 0, atc: 0 };
      rows.push([targetYmd, h, v.sessions, v.atc, null]);
    }

    const placeholders = rows.map(() => "(?, ?, ?, ?, ?)").join(", ");
    await conn.query(
      `
      INSERT INTO hourly_sessions_summary_shopify
        (date, hour, number_of_sessions, number_of_atc_sessions, adjusted_number_of_sessions)
      VALUES ${placeholders}
      `,
      rows.flat()
    );

    console.log(`[${brand.tag}] Upserted hourly summary for ${targetYmd}`);
  } finally {
    conn.release();
  }
}

// ---------- run ----------
async function run() {
  if (!MODE) {
    console.log(`[HOURLY] HOURLY_BACKFILL_MODE is not true. Exiting.`);
    return;
  }
  if (!START || !END) throw new Error(`[HOURLY] Missing HOURLY_BACKFILL_START_IST_DATE or HOURLY_BACKFILL_END_IST_DATE`);

  const dates = buildInclusiveDateRangeYMD(START, END);
  const brands = loadBrands();

  console.log(`[HOURLY] Backfill range ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} days)`);
  console.log(`[HOURLY] Timezone for hour_of_day grouping: ${SHOPIFYQL_TIMEZONE}`);

  for (const brand of brands) {
    await ensureHourlyTable(brand);
  }

  for (const d of dates) {
    console.log(`\n[HOURLY] ===== ${d} =====`);
    await Promise.all(
      brands.map(async (brand) => {
        const hourly = await fetchShopifyQLHourlySessions(brand, d);
        await upsertHourlySessionsSummary(brand, hourly, d);
      })
    );
  }

  console.log(`\n[HOURLY] Done.`);
}

run().catch((e) => {
  console.error(`[HOURLY] Crash:`, e);
  process.exit(1);
});
