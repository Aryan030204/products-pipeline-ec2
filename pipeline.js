/**
 * Product Sessions ETL Pipeline (with Backfill + Hourly Summary)
 *
 * Normal mode (hourly cron):
 *   BACKFILL_MODE=false (or unset)
 *
 * Backfill mode (runs once on startup, cron disabled):
 *   BACKFILL_MODE=true
 *   BACKFILL_START_IST_DATE=2025-10-01
 *   BACKFILL_END_IST_DATE=2025-12-14
 *
 * Optional fallback:
 *   SHOPIFYQL_TIMEZONE=Asia/Kolkata   (used only when a brand has no valid store_timezone)
 *
 * Hourly product sessions (full dimensions):
 *   HOURLY_PRODUCT_SESSIONS_ENABLED=true
 *   HOURLY_PRODUCT_SESSIONS_BACKFILL_ENABLED=true   (only if you want this to run in BACKFILL_MODE)
 */

import "dotenv/config";
import dotenv from "dotenv";
import mysql from "mysql2/promise";
import axios from "axios";
import cron from "node-cron";
import fs from "fs";
import express from "express";
import { pathToFileURL } from "url";
import crypto from "crypto";
import {
  todayYMDInTimeZone,
  sleep,
  formatShopifyQLTable,
  buildDayClause,
  ShopifyQLFetchError,
  getThrottleResetSleepMsFromGraphQLErrors,
} from "./shopifyql_utils.js";
import {
  ensureProductTypeSalesTables,
  fetchProductTypeSales,
  upsertProductTypeSalesSnapshot,
  refreshProductTypeFunnelRollup,
} from "./product_type_sales.js";

// ---------- Runtime log capture (last 2 lines) ----------
const RUNTIME_LOG_LIMIT = 2;
const runtimeLogs = [];

function formatLogArg(arg) {
  if (typeof arg === "string") return arg;
  if (arg instanceof Error) return arg.stack || arg.message || String(arg);
  try {
    return JSON.stringify(arg);
  } catch {
    return String(arg);
  }
}

function pushRuntimeLog(level, args) {
  const line = `[${level}] ${args.map(formatLogArg).join(" ")}`.trim();
  if (!line) return;
  runtimeLogs.push(line);
  while (runtimeLogs.length > RUNTIME_LOG_LIMIT) runtimeLogs.shift();
}

const baseConsole = {
  log: console.log.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};

console.log = (...args) => {
  pushRuntimeLog("INFO", args);
  baseConsole.log(...args);
};

console.warn = (...args) => {
  pushRuntimeLog("WARN", args);
  baseConsole.warn(...args);
};

console.error = (...args) => {
  pushRuntimeLog("ERROR", args);
  baseConsole.error(...args);
};

function getLastRuntimeLogs() {
  return runtimeLogs.slice(-RUNTIME_LOG_LIMIT).join("\n");
}

function normalizeErrorCode(value) {
  const normalized = String(value || "")
    .replace(/[^a-z0-9]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
  return normalized || "UNKNOWN_ERROR";
}

function deriveErrorCode(err) {
  const code = err?.code || err?.name || "Error";
  const message = String(err?.message || "");
  const combined = `${code} ${message}`.toLowerCase();

  if (combined.includes("timeout")) return "DB_TIMEOUT";
  if (combined.includes("access denied") || combined.includes("er_access_denied_error")) {
    return "DB_AUTH_FAILED";
  }
  if (combined.includes("econnrefused") || combined.includes("enotfound")) {
    return "NETWORK_ERROR";
  }
  if (combined.includes("env") || combined.includes("missing") || combined.includes("password_aes_key")) {
    return "ENV_ERROR";
  }

  return normalizeErrorCode(code);
}

// ---------- Time helpers ----------
const IST_OFFSET_MIN = 330; // +05:30

function nowIST() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  return new Date(utc + IST_OFFSET_MIN * 60000);
}

function fmtIST() {
  const d = nowIST();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}+05:30`;
}

function todayISTDate() {
  const d = nowIST();
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function fmtDate(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function todayISTYMD() {
  return fmtDate(todayISTDate());
}

function isValidTimeZone(timeZone) {
  if (!timeZone) return false;
  try {
    Intl.DateTimeFormat("en-US", { timeZone }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function extractIanaTimeZone(storeTimezoneValue) {
  const raw = String(storeTimezoneValue || "").trim();
  if (!raw) return null;
  if (isValidTimeZone(raw)) return raw;

  const match = raw.match(/([A-Za-z_]+(?:\/[A-Za-z0-9_+\-]+)+)$/);
  if (!match) return null;

  const candidate = match[1];
  return isValidTimeZone(candidate) ? candidate : null;
}

function resolveBrandTimeZone(storeTimezoneValue, brandName = "unknown") {
  const parsed = extractIanaTimeZone(storeTimezoneValue);
  if (parsed) return parsed;

  if (storeTimezoneValue) {
    console.warn(
      `[INIT] Brand ${brandName} has invalid store_timezone="${storeTimezoneValue}". Falling back to ${DEFAULT_SHOPIFYQL_TIMEZONE}.`,
    );
  }

  return DEFAULT_SHOPIFYQL_TIMEZONE;
}

function parseBrandIndexList(rawValue) {
  if (!rawValue) return null;

  const unique = new Set();
  for (const part of rawValue.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    if (!/^\d+$/.test(trimmed)) {
      throw new Error(
        `[BACKFILL] Invalid BACKFILL_BRAND_INDICES="${rawValue}". Expected a comma-separated list of non-negative integers.`,
      );
    }
    unique.add(parseInt(trimmed, 10));
  }

  return unique;
}

export function selectBackfillBrands(brands) {
  const selectedIndices = parseBrandIndexList(BACKFILL_BRAND_INDICES);
  if (!selectedIndices || selectedIndices.size === 0) {
    return brands;
  }

  const filteredBrands = brands.filter((brand) => selectedIndices.has(brand.index));
  if (!filteredBrands.length) {
    throw new Error(
      `[BACKFILL] BACKFILL_BRAND_INDICES="${BACKFILL_BRAND_INDICES}" matched no active brands. Active brand indices: ${brands.map((brand) => brand.index).join(", ") || "none"}`,
    );
  }

  const availableIndices = new Set(filteredBrands.map((brand) => brand.index));
  const missingIndices = [...selectedIndices].filter(
    (index) => !availableIndices.has(index),
  );

  console.log(
    `[BACKFILL] Brand filter enabled via BACKFILL_BRAND_INDICES=${BACKFILL_BRAND_INDICES}. Selected brands: ${filteredBrands.map((brand) => `${brand.index}:${brand.name}`).join(", ")}`,
  );
  if (missingIndices.length) {
    console.warn(
      `[BACKFILL] Ignoring requested brand indices with no active match: ${missingIndices.join(", ")}`,
    );
  }

  return filteredBrands;
}

// ---------- Backfill env ----------
const BACKFILL_MODE =
  String(process.env.BACKFILL_MODE || "")
    .toLowerCase()
    .trim() === "true";
const BACKFILL_START_IST_DATE = (
  process.env.BACKFILL_START_IST_DATE ||
  process.env.BACKFILL_START_IST ||
  ""
).split("T")[0]; // YYYY-MM-DD
const BACKFILL_END_IST_DATE = (
  process.env.BACKFILL_END_IST_DATE ||
  process.env.BACKFILL_END_IST ||
  ""
).split("T")[0]; // YYYY-MM-DD
const BACKFILL_BRAND_INDICES = String(
  process.env.BACKFILL_BRAND_INDICES || "",
).trim();
// Rolling backfill window. When > 0, the backfill ignores the fixed START/END
// dates above and instead refetches each brand's own last N *brand-local* days
// (end = today in the brand's timezone, start = end - (N-1)). This keeps the
// window correct for brands in different timezones. Explicit START/END is still
// used for one-off historical backfills when this is 0/unset.
const BACKFILL_LOOKBACK_DAYS = Math.max(
  0,
  parseInt(process.env.BACKFILL_LOOKBACK_DAYS || "0", 10) || 0,
);
// One-shot mode for CI/cron runners: after the backfill completes, exit the
// process (instead of leaving the HTTP server running). Exit code is always 0 —
// per-brand/per-day failures are logged and skipped by design, not fatal.
const EXIT_AFTER_BACKFILL =
  String(process.env.EXIT_AFTER_BACKFILL || "").toLowerCase().trim() === "true";
const DEFAULT_SHOPIFYQL_TIMEZONE =
  process.env.SHOPIFYQL_TIMEZONE || "Asia/Kolkata";
const TEST_MODE = process.env.TEST_MODE === "true";
const BACKFILL_BRAND_CONCURRENCY = Math.max(
  1,
  parseInt(process.env.BACKFILL_BRAND_CONCURRENCY || "2", 10) || 2,
);
const HOURLY_BRAND_CONCURRENCY = Math.max(
  1,
  parseInt(process.env.HOURLY_BRAND_CONCURRENCY || "2", 10) || 2,
);
const HOURLY_PRODUCT_SESSIONS_ENABLED =
  String(process.env.HOURLY_PRODUCT_SESSIONS_ENABLED || "").toLowerCase().trim() === "true";
const HOURLY_PRODUCT_SESSIONS_BACKFILL_ENABLED =
  String(process.env.HOURLY_PRODUCT_SESSIONS_BACKFILL_ENABLED || "").toLowerCase().trim() === "true";
const HOURLY_PRODUCT_SESSIONS_RETENTION_DAYS =
  parseInt(process.env.HOURLY_PRODUCT_SESSIONS_RETENTION_DAYS || "7", 10);
const SELF_RUN =
  String(process.env.SELF_RUN || "true").toLowerCase().trim() === "true";

// Daily sessions fetch paginates via ShopifyQL LIMIT/OFFSET (a single query is
// capped at 1000 rows, which truncated the long tail and undercounted per-page
// sessions). Page size is the per-request LIMIT; max pages is a runaway guard
// (SHOPIFYQL_MAX_PAGES * SHOPIFYQL_PAGE_SIZE rows). If a page exhausts its
// retries — or we blow past the page cap — the whole day is skipped.
const SHOPIFYQL_PAGE_SIZE = Math.max(
  1,
  parseInt(process.env.SHOPIFYQL_PAGE_SIZE || "1000", 10) || 1000,
);
const SHOPIFYQL_MAX_PAGES = Math.max(
  1,
  parseInt(process.env.SHOPIFYQL_MAX_PAGES || "50", 10) || 50,
);
const SHOPIFYQL_FETCH_MAX_RETRIES = Math.max(
  1,
  parseInt(process.env.SHOPIFYQL_FETCH_MAX_RETRIES || "5", 10) || 5,
);

// ---------- Concurrency helper ----------
export async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const idx = nextIndex;
      nextIndex += 1;
      if (idx >= items.length) return;
      results[idx] = await mapper(items[idx], idx);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker(),
  );
  await Promise.all(workers);
  return results;
}

// ---------- Date-range helpers (safe iteration using UTC) ----------
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

export function buildInclusiveDateRangeYMD(startYmd, endYmd) {
  if (!isValidYMD(startYmd) || !isValidYMD(endYmd)) {
    throw new Error(
      `[BACKFILL] Invalid date format. Expected YYYY-MM-DD for BACKFILL_START_IST_DATE/BACKFILL_END_IST_DATE. Got start=${startYmd}, end=${endYmd}`,
    );
  }

  const start = ymdToUTCDate(startYmd);
  const end = ymdToUTCDate(endYmd);

  if (start.getTime() > end.getTime()) {
    throw new Error(
      `[BACKFILL] Start date is after end date: ${startYmd} > ${endYmd}`,
    );
  }

  const out = [];
  for (
    let cur = start;
    cur.getTime() <= end.getTime();
    cur = addDaysUTC(cur, 1)
  ) {
    out.push(fmtUTCDateToYMD(cur));
  }
  return out;
}

// Pure label arithmetic (no timezone involved) — shift a YYYY-MM-DD by `days`.
function shiftYMD(ymd, days) {
  return fmtUTCDateToYMD(addDaysUTC(ymdToUTCDate(ymd), days));
}

// The last N brand-local calendar days, as YYYY-MM-DD labels, inclusive of the
// brand's "today". These labels are later interpreted in the brand's own
// timezone by the ShopifyQL WITH TIMEZONE clause.
export function recentDatesForTimeZone(timeZone, lookbackDays) {
  const end = todayYMDInTimeZone(timeZone);
  const start = shiftYMD(end, -(lookbackDays - 1));
  return buildInclusiveDateRangeYMD(start, end);
}

// ---------- API / Decryption helpers ----------
const GET_BRANDS_API = process.env.GET_BRANDS_API;
const PIPELINE_AUTH_HEADER = process.env.PIPELINE_AUTH_HEADER;
const PASSWORD_AES_KEY = process.env.PASSWORD_AES_KEY;
const API_HEADERS = { "x-pipeline-key": PIPELINE_AUTH_HEADER };

function normalizeKey(rawKey) {
  let buf = Buffer.from(rawKey, "utf8");
  if (buf.length < 32) {
    const padded = Buffer.alloc(32); // zero-filled
    buf.copy(padded);
    buf = padded;
  } else if (buf.length > 32) {
    buf = buf.slice(0, 32);
  }
  return buf;
}

function decryptAES(encryptedValue) {
  if (!encryptedValue) return encryptedValue;
  if (!encryptedValue.includes(":")) return encryptedValue;

  try {
    const [ivB64, encryptedB64] = encryptedValue.split(":");
    const iv = Buffer.from(ivB64, "base64");
    const encrypted = Buffer.from(encryptedB64, "base64");
    const key = normalizeKey(PASSWORD_AES_KEY);

    const decipher = crypto.createDecipheriv("aes-256-cbc", key, iv);
    let decrypted = decipher.update(encrypted, undefined, "utf8");
    decrypted += decipher.final("utf8");
    return decrypted;
  } catch (err) {
    console.error(`[DECRYPT ERROR] Failed to decrypt value: ${err.message}`);
    throw err;
  }
}

function decryptAESMaybe(encryptedValue) {
  if (!encryptedValue) return encryptedValue;
  if (!encryptedValue.includes(":")) return encryptedValue;
  if (!PASSWORD_AES_KEY) {
    throw new Error(
      "[INIT] PASSWORD_AES_KEY is required to decrypt encrypted values from .env.local",
    );
  }
  return decryptAES(encryptedValue);
}

function pickField(obj, keys) {
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) return obj[key];
  }
  return undefined;
}

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildBrandListFromIndexedEnv(envLocal) {
  const explicitIndexes = new Set(
    Object.keys(envLocal)
      .map((key) =>
        key.match(/^(?:BRAND_ID|SHOP_NAME|ACCESS_TOKEN|DB_HOST)_(\d+)$/),
      )
      .filter(Boolean)
      .map((match) => Number(match[1])),
  );

  const activeBrandIds = parseCsv(envLocal.ACTIVE_BRAND_IDS);
  if (activeBrandIds.length) {
    for (let index = 0; index < activeBrandIds.length; index++) {
      explicitIndexes.add(index);
    }
  }

  return Array.from(explicitIndexes)
    .sort((a, b) => a - b)
    .map((index) => ({
      id: envLocal[`BRAND_ID_${index}`] || activeBrandIds[index] || index + 1,
      shop_name: envLocal[`SHOP_NAME_${index}`],
      api_version: envLocal[`API_VERSION_${index}`] || envLocal.API_VERSION,
      access_token: envLocal[`ACCESS_TOKEN_${index}`],
      db_host: envLocal[`DB_HOST_${index}`],
      db_user: envLocal[`DB_USER_${index}`],
      db_password: envLocal[`DB_PASSWORD_${index}`],
      db_database: envLocal[`DB_DATABASE_${index}`],
      brand_tag: envLocal[`BRAND_TAG_${index}`],
      brand_name: envLocal[`BRAND_NAME_${index}`],
      store_timezone: envLocal[`STORE_TIMEZONE_${index}`],
    }));
}

async function loadBrandsFromEnvLocal() {
  const envLocalPath = new URL(".env.local", import.meta.url);
  if (!fs.existsSync(envLocalPath)) {
    console.error("[INIT] TEST_MODE is true but .env.local was not found.");
    process.exit(1);
  }

  let envLocal;
  try {
    const raw = fs.readFileSync(envLocalPath, "utf8");
    envLocal = dotenv.parse(raw);
  } catch (err) {
    console.error("[INIT] Failed to read .env.local:", err.message);
    process.exit(1);
  }

  let brandList;
  const brandsJson = envLocal.BRANDS_JSON;
  if (brandsJson) {
    try {
      brandList = JSON.parse(brandsJson);
    } catch (err) {
      console.error("[INIT] Failed to parse BRANDS_JSON:", err.message);
      process.exit(1);
    }
  } else {
    brandList = buildBrandListFromIndexedEnv(envLocal);
  }

  if (!Array.isArray(brandList) || brandList.length === 0) {
    console.error(
      "[INIT] .env.local must define BRANDS_JSON or indexed brand variables like BRAND_ID_0, SHOP_NAME_0, ACCESS_TOKEN_0, DB_HOST_0 when TEST_MODE is true.",
    );
    process.exit(1);
  }

  console.log(`[INIT] Loading ${brandList.length} brands from .env.local...`);

  const brands = [];
  let index = 0;

  for (const brandData of brandList) {
    const idRaw = pickField(brandData, ["id", "brand_id"]);
    const id = Number.isFinite(Number(idRaw)) ? Number(idRaw) : index + 1;

    const shopName = pickField(brandData, ["shop_name", "shopName"]);
    const apiVersion = pickField(brandData, ["api_version", "apiVersion"]);
    const dbHost = pickField(brandData, ["db_host", "dbHost"]);
    const dbUser = pickField(brandData, ["db_user", "dbUser"]);
    const dbDatabase = pickField(brandData, ["db_database", "dbDatabase"]);
    const brandTag =
      pickField(brandData, ["brand_tag", "brandTag"]) || `brand_${index}`;
    const brandName =
      pickField(brandData, ["brand_name", "brandName"]) ||
      brandTag.toUpperCase();
    const storeTimezone = pickField(brandData, [
      "store_timezone",
      "storeTimezone",
    ]);
    const shopifyQlTimezone = resolveBrandTimeZone(storeTimezone, brandName);

    let accessToken, dbPassword;
    try {
      accessToken = decryptAESMaybe(
        pickField(brandData, ["access_token", "accessToken"]),
      );
      dbPassword = decryptAESMaybe(
        pickField(brandData, ["db_password", "dbPassword"]),
      );
    } catch (err) {
      console.error(
        `[INIT] Skipping brand ${brandName} – failed to decrypt credentials.`,
      );
      continue;
    }

    const dbSslEnabledEnv = String(
      process.env.DB_SSL_ENABLED || "",
    ).toLowerCase();
    let dbSslEnabled = dbSslEnabledEnv === "true";

    if (
      !dbSslEnabled &&
      dbHost &&
      dbSslEnabledEnv === "" &&
      /amazonaws\.com$/i.test(dbHost) &&
      !TEST_MODE
    ) {
      dbSslEnabled = true;
    }

    if (TEST_MODE) {
      console.warn(
        `[INIT] Brand [${brandName}] TEST MODE ENABLED: SSL explicitly disabled.`,
      );
    }

    const dbSslCa = process.env.DB_SSL_CA;
    const dbSslCaFile = process.env.DB_SSL_CA_FILE;
    let ssl;
    if (dbSslEnabled || dbSslCa || dbSslCaFile) {
      let ca = dbSslCa ? dbSslCa.replace(/\\n/g, "\n") : undefined;
      if (!ca && dbSslCaFile) {
        try {
          ca = fs.readFileSync(dbSslCaFile, "utf8");
        } catch (err) {
          console.warn(
            `[INIT] Failed to read CA file for brand ${brandName} (${dbSslCaFile}): ${err?.message}`,
          );
        }
      }

      ssl = {
        rejectUnauthorized: false,
        ...(ca ? { ca } : {}),
      };
    }

    if (
      !shopName ||
      !apiVersion ||
      !accessToken ||
      !dbHost ||
      !dbUser ||
      !dbDatabase
    ) {
      console.warn(
        `[INIT] Skipping brand ${brandName} – missing required fields.`,
      );
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
      ...(ssl && !TEST_MODE ? { ssl } : {}),
    });

    brands.push({
      index,
      id,
      tag: brandTag,
      name: brandName,
      shopName,
      apiVersion,
      accessToken,
      storeTimezone,
      shopifyQlTimezone,
      dbDatabase,
      pool,
      _tablesEnsured: false,
    });

    console.log(
      `[INIT] Brand[${index}] ${brandName} ready (shop=${shopName}, db=${dbDatabase}, tz=${shopifyQlTimezone})`,
    );
    index++;
  }

  console.log(
    `[INIT] Active brands: ${brands.map((b) => `${b.index}:${b.name}`).join(", ")}`,
  );
  return brands;
}

// ---------- Brand config ----------
async function loadBrands() {
  if (TEST_MODE) {
    return loadBrandsFromEnvLocal();
  }

  if (!GET_BRANDS_API || !PIPELINE_AUTH_HEADER || !PASSWORD_AES_KEY) {
    console.error(
      "[INIT] Missing required environment variables: GET_BRANDS_API, PIPELINE_AUTH_HEADER, PASSWORD_AES_KEY",
    );
    process.exit(1);
  }

  console.log(`[INIT] Fetching brands from ${GET_BRANDS_API}...`);
  let brandDict = {};
  try {
    const resp = await axios.get(GET_BRANDS_API, { headers: API_HEADERS });
    brandDict = resp.data;
  } catch (err) {
    console.error("[INIT] Failed to fetch brand list:", err.message);
    process.exit(1);
  }

  const brandIds = Object.keys(brandDict);
  const count = brandIds.length;
  console.log(`[INIT] Loading ${count} brands...`);

  const brands = [];
  let index = 0;
  let inactiveCount = 0;

  for (const strId of brandIds) {
    const id = parseInt(strId, 10);
    const brandUrl = `${GET_BRANDS_API}/${id}`;

    let brandData;
    try {
      const resp = await axios.get(brandUrl, { headers: API_HEADERS });
      brandData = resp.data;
    } catch (err) {
      console.error(
        `[INIT] Failed to fetch credentials for brand ID ${id}:`,
        err.message,
      );
      continue;
    }

    if (brandData?.is_active !== true) {
      const brandLabel =
        brandData?.brand_name || brandData?.brand_tag || `brand_id_${id}`;
      console.log(
        `[INIT] Skipping brand ${brandLabel} (ID ${id}) - is_active is not true.`,
      );
      inactiveCount++;
      continue;
    }

    const shopName = brandData.shop_name;
    const apiVersion = brandData.api_version;
    const dbHost = brandData.db_host;
    const dbUser = brandData.db_user;
    const dbDatabase = brandData.db_database;
    const brandTag = brandData.brand_tag || `brand_${index}`;
    const brandName = brandData.brand_name || brandTag.toUpperCase();
    const storeTimezone = brandData.store_timezone;
    const shopifyQlTimezone = resolveBrandTimeZone(storeTimezone, brandName);

    let accessToken, dbPassword;
    try {
      accessToken = decryptAES(brandData.access_token);
      dbPassword = decryptAES(brandData.db_password);
    } catch (err) {
      console.error(
        `[INIT] Skipping brand ${brandName} – failed to decrypt credentials.`,
      );
      continue;
    }

    // Rely on global DB_SSL_ENABLED environment variable
    const dbSslEnabledEnv = String(
      process.env.DB_SSL_ENABLED || "",
    ).toLowerCase();
    let dbSslEnabled = dbSslEnabledEnv === "true";

    // Auto-enable for RDS/Proxy if no global flag is set
    if (
      !dbSslEnabled &&
      dbHost &&
      dbSslEnabledEnv === "" &&
      /amazonaws\.com$/i.test(dbHost) &&
      !TEST_MODE
    ) {
      dbSslEnabled = true;
    }

    if (TEST_MODE) {
      console.warn(
        `[INIT] Brand [${brandName}] TEST MODE ENABLED: SSL explicitly disabled.`,
      );
    }

    const dbSslCa = process.env.DB_SSL_CA;
    const dbSslCaFile = process.env.DB_SSL_CA_FILE;
    let ssl;
    if (dbSslEnabled || dbSslCa || dbSslCaFile) {
      let ca = dbSslCa ? dbSslCa.replace(/\\n/g, "\n") : undefined;
      if (!ca && dbSslCaFile) {
        try {
          ca = fs.readFileSync(dbSslCaFile, "utf8");
        } catch (err) {
          console.warn(
            `[INIT] Failed to read CA file for brand ${brandName} (${dbSslCaFile}): ${err?.message}`,
          );
        }
      }

      ssl = {
        rejectUnauthorized: false,
        ...(ca ? { ca } : {}),
      };
    }

    if (
      !shopName ||
      !apiVersion ||
      !accessToken ||
      !dbHost ||
      !dbUser ||
      !dbDatabase
    ) {
      console.warn(
        `[INIT] Skipping brand ${brandName} – missing required fields.`,
      );
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
      ...(ssl && !TEST_MODE ? { ssl } : {}),
    });

    brands.push({
      index,
      id,
      tag: brandTag,
      name: brandName,
      shopName,
      apiVersion,
      accessToken,
      storeTimezone,
      shopifyQlTimezone,
      dbDatabase,
      pool,
      _tablesEnsured: false,
    });

    console.log(
      `[INIT] Brand[${index}] ${brandName} ready (shop=${shopName}, db=${dbDatabase}, tz=${shopifyQlTimezone})`,
    );
    index++;
  }

  console.log(`[INIT] Inactive brands skipped: ${inactiveCount}`);
  console.log(
    `[INIT] Active brands: ${brands.map((b) => `${b.index}:${b.name}`).join(", ")}`,
  );
  return brands;
}

// Reuse pools across runs
let _BRANDS = null;
export async function getBrands() {
  if (!_BRANDS) _BRANDS = await loadBrands();
  return _BRANDS;
}

async function ensureIndex(conn, tableName, indexName, addIndexSql) {
  const [rows] = await conn.query(`SHOW INDEX FROM ?? WHERE Key_name = ?`, [
    tableName,
    indexName,
  ]);
  if (!rows.length) {
    await conn.query(addIndexSql);
  }
}

async function ensureColumn(conn, tableName, columnName, addColumnSql) {
  const [rows] = await conn.query(`SHOW COLUMNS FROM ?? LIKE ?`, [
    tableName,
    columnName,
  ]);
  if (!rows.length) {
    await conn.query(addColumnSql);
  }
}

// Stored hash of the dimension tuple — must stay in lock-step across
// product_sessions_snapshot and hourly_product_sessions so identical tuples
// produce identical hashes.
const DIM_HASH_EXPR = `UNHEX(SHA1(CONCAT_WS('|',
  landing_page_type,
  landing_page_path,
  IFNULL(utm_source,''),
  IFNULL(utm_medium,''),
  IFNULL(utm_campaign,''),
  IFNULL(utm_content,''),
  IFNULL(utm_term,''),
  IFNULL(referrer_name,'')
)))`;

const PATH_NORM_EXPR = `IF(landing_page_path='/', '/', TRIM(TRAILING '/' FROM landing_page_path))`;

// ---------- DB Setup ----------
export async function ensureTablesForBrand(brand) {
  if (brand._tablesEnsured) return;

  const conn = await brand.pool.getConnection();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_sessions_snapshot (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        date DATE NOT NULL,
        landing_page_type VARCHAR(100) NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        product_title VARCHAR(255) DEFAULT NULL,

        utm_source   VARCHAR(255) NULL,
        utm_medium   VARCHAR(255) NULL,
        utm_campaign VARCHAR(255) NULL,
        utm_content  VARCHAR(255) NULL,
        utm_term     VARCHAR(255) NULL,
        referrer_name VARCHAR(255) NULL,

        sessions INT DEFAULT 0,
        sessions_with_cart_additions INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        landing_page_path_norm VARCHAR(500) GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED,
        dim_hash BINARY(20) GENERATED ALWAYS AS (${DIM_HASH_EXPR}) STORED,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_date_dim (date, dim_hash),
        KEY idx_date (date),
        KEY idx_date_norm (date, landing_page_path_norm(200)),
        KEY idx_date_updated (date, updated_at),
        KEY idx_product_id (product_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Provision the new upsert-path columns + indexes for existing tables
    // that predate this schema. All idempotent.
    await ensureColumn(
      conn,
      "product_sessions_snapshot",
      "updated_at",
      `ALTER TABLE product_sessions_snapshot
         ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`,
    );
    await ensureColumn(
      conn,
      "product_sessions_snapshot",
      "landing_page_path_norm",
      `ALTER TABLE product_sessions_snapshot
         ADD COLUMN landing_page_path_norm VARCHAR(500)
           GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED`,
    );
    await ensureColumn(
      conn,
      "product_sessions_snapshot",
      "dim_hash",
      `ALTER TABLE product_sessions_snapshot
         ADD COLUMN dim_hash BINARY(20)
           GENERATED ALWAYS AS (${DIM_HASH_EXPR}) STORED`,
    );
    await ensureIndex(
      conn,
      "product_sessions_snapshot",
      "uniq_date_dim",
      `ALTER TABLE product_sessions_snapshot
         ADD UNIQUE KEY uniq_date_dim (date, dim_hash)`,
    );
    await ensureIndex(
      conn,
      "product_sessions_snapshot",
      "idx_date_norm",
      `ALTER TABLE product_sessions_snapshot
         ADD KEY idx_date_norm (date, landing_page_path_norm(200))`,
    );
    await ensureIndex(
      conn,
      "product_sessions_snapshot",
      "idx_date_updated",
      `ALTER TABLE product_sessions_snapshot
         ADD KEY idx_date_updated (date, updated_at)`,
    );

    const [snapshotCols] = await conn.query(
      `SHOW COLUMNS FROM product_sessions_snapshot LIKE 'product_id'`,
    );
    if (snapshotCols.length === 0) {
      console.log(
        `[${brand.name}] 'product_id' column missing in product_sessions_snapshot. Adding it...`,
      );
      await conn.query(
        `ALTER TABLE product_sessions_snapshot ADD COLUMN product_id VARCHAR(50) NULL AFTER landing_page_path`,
      );
      await conn.query(
        `ALTER TABLE product_sessions_snapshot ADD KEY idx_product_id (product_id)`,
      );
    }

    const [snapshotTitleCols] = await conn.query(
      `SHOW COLUMNS FROM product_sessions_snapshot LIKE 'product_title'`,
    );
    if (snapshotTitleCols.length === 0) {
      console.log(
        `[${brand.name}] 'product_title' column missing in product_sessions_snapshot. Adding it...`,
      );
      await conn.query(
        `ALTER TABLE product_sessions_snapshot ADD COLUMN product_title VARCHAR(255) NULL AFTER product_id`,
      );
    }

    // Check for product_type in MV.
    const [mvCols] = await conn.query(
      `SHOW COLUMNS FROM mv_product_sessions_by_type_daily LIKE 'product_type'`,
    );
    if (mvCols.length === 0) {
      console.log(
        `[${brand.name}] Updating mv_product_sessions_by_type_daily schema (ADD column + Update PK)...`,
      );
      try {
        await conn.query(
          `ALTER TABLE mv_product_sessions_by_type_daily ADD COLUMN product_type VARCHAR(255) NOT NULL DEFAULT 'Unknown' AFTER landing_page_type`,
        );
        await conn.query(
          `ALTER TABLE mv_product_sessions_by_type_daily DROP PRIMARY KEY, ADD PRIMARY KEY (date, landing_page_type, product_type)`,
        );
      } catch (err) {
        console.warn(
          `[${brand.name}] Schema update warning (might have partially run or PK issue): ${err.message}`,
        );
        // If generic ALTER fails, we might technically need manual intervention or just let it crash, but listing it here is safer.
      }
    }

    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_type_daily (
        date DATE NOT NULL,
        landing_page_type VARCHAR(100) NOT NULL,
        product_type VARCHAR(255) NOT NULL DEFAULT 'Unknown',
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, landing_page_type, product_type),
        KEY idx_type (landing_page_type)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Note: includes add_to_cart_rate_pct because your INSERT uses it
    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_path_daily (
        date DATE NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
        add_to_cart_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
        conversion_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, landing_page_path(200)),
        KEY idx_date (date),
        KEY idx_sessions (date, sessions DESC),
        KEY idx_product_id (product_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS product_landing_mapping (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        product_id BIGINT NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        status VARCHAR(50) DEFAULT NULL,
        title VARCHAR(255) DEFAULT NULL,
        last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        landing_page_path_norm VARCHAR(500) GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_landing_page_path (landing_page_path(200)),
        UNIQUE KEY uniq_landing_page_path_norm (landing_page_path_norm(200)),
        KEY idx_product_id (product_id),
        KEY idx_last_synced_at (last_synced_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Check for product_type column and add if missing
    const [cols] = await conn.query(
      `SHOW COLUMNS FROM product_landing_mapping LIKE 'product_type'`,
    );
    if (cols.length === 0) {
      console.log(
        `[${brand.name}] 'product_type' column missing in product_landing_mapping. Adding it...`,
      );
      await conn.query(
        `ALTER TABLE product_landing_mapping ADD COLUMN product_type VARCHAR(255) NULL AFTER title`,
      );
    }

    await ensureColumn(
      conn,
      "product_landing_mapping",
      "landing_page_path_norm",
      `ALTER TABLE product_landing_mapping
         ADD COLUMN landing_page_path_norm VARCHAR(500)
           GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED`,
    );
    await ensureIndex(
      conn,
      "product_landing_mapping",
      "uniq_landing_page_path_norm",
      `ALTER TABLE product_landing_mapping
         ADD UNIQUE KEY uniq_landing_page_path_norm (landing_page_path_norm(200))`,
    );

    // Note: includes landing_page_path because your INSERT uses it
    await conn.query(`
      CREATE TABLE IF NOT EXISTS mv_product_sessions_by_campaign_daily (
        date DATE NOT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        utm_campaign VARCHAR(255) NOT NULL,
        product_id   VARCHAR(50)  DEFAULT NULL,
        referrer_name VARCHAR(255) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        add_to_cart_rate_pct   DECIMAL(7,4) NOT NULL DEFAULT 0,
        conversion_rate_pct    DECIMAL(7,4) NOT NULL DEFAULT 0,
        PRIMARY KEY (date, utm_campaign, landing_page_path(200), product_id, referrer_name(100)),
        KEY idx_date      (date),
        KEY idx_campaign  (utm_campaign),
        KEY idx_product   (product_id),
        KEY idx_referrer  (referrer_name(100))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await conn.query(`
      CREATE TABLE IF NOT EXISTS pipeline_metadata (
        key_name VARCHAR(100) PRIMARY KEY,
        key_value VARCHAR(255) NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    // Hourly summary table (your requested target)
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

    // Hourly product sessions with full dimensions (feature-flagged ingestion)
    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_product_sessions (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        landing_page_type VARCHAR(100) DEFAULT NULL,
        landing_page_path VARCHAR(500) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        product_title VARCHAR(255) DEFAULT NULL,
        utm_source VARCHAR(255) DEFAULT NULL,
        utm_medium VARCHAR(255) DEFAULT NULL,
        utm_campaign VARCHAR(255) DEFAULT NULL,
        utm_content VARCHAR(255) DEFAULT NULL,
        utm_term VARCHAR(255) DEFAULT NULL,
        referrer_name VARCHAR(255) DEFAULT NULL,
        sessions INT NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        landing_page_path_norm VARCHAR(500) GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED,
        dim_hash BINARY(20) GENERATED ALWAYS AS (${DIM_HASH_EXPR}) STORED,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_date_hour_dim (date, hour, dim_hash),
        KEY idx_date_hour (date, hour),
        KEY idx_date_norm (date, landing_page_path_norm(200)),
        KEY idx_date_updated (date, updated_at),
        KEY idx_product_date (product_id, date)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await ensureColumn(
      conn,
      "hourly_product_sessions",
      "updated_at",
      `ALTER TABLE hourly_product_sessions
         ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`,
    );
    await ensureColumn(
      conn,
      "hourly_product_sessions",
      "landing_page_path_norm",
      `ALTER TABLE hourly_product_sessions
         ADD COLUMN landing_page_path_norm VARCHAR(500)
           GENERATED ALWAYS AS (${PATH_NORM_EXPR}) STORED`,
    );
    await ensureColumn(
      conn,
      "hourly_product_sessions",
      "dim_hash",
      `ALTER TABLE hourly_product_sessions
         ADD COLUMN dim_hash BINARY(20)
           GENERATED ALWAYS AS (${DIM_HASH_EXPR}) STORED`,
    );
    await ensureIndex(
      conn,
      "hourly_product_sessions",
      "uniq_date_hour_dim",
      `ALTER TABLE hourly_product_sessions
         ADD UNIQUE KEY uniq_date_hour_dim (date, hour, dim_hash)`,
    );
    await ensureIndex(
      conn,
      "hourly_product_sessions",
      "idx_date_norm",
      `ALTER TABLE hourly_product_sessions
         ADD KEY idx_date_norm (date, landing_page_path_norm(200))`,
    );
    await ensureIndex(
      conn,
      "hourly_product_sessions",
      "idx_date_updated",
      `ALTER TABLE hourly_product_sessions
         ADD KEY idx_date_updated (date, updated_at)`,
    );

    await conn.query(`
      CREATE TABLE IF NOT EXISTS hourly_product_performance_rollup (
        date DATE NOT NULL,
        hour TINYINT UNSIGNED NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        product_title VARCHAR(255) DEFAULT NULL,
        sessions INT UNSIGNED NOT NULL DEFAULT 0,
        sessions_with_cart_additions INT UNSIGNED NOT NULL DEFAULT 0,
        orders INT UNSIGNED NOT NULL DEFAULT 0,
        units_sold INT UNSIGNED NOT NULL DEFAULT 0,
        total_sales DECIMAL(14,2) NOT NULL DEFAULT 0.00,
        add_to_cart_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
        cvr DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, hour, product_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);

    await ensureIndex(
      conn,
      "hourly_product_performance_rollup",
      "idx_hpr_product_date_hour",
      "ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_product_date_hour (product_id, date, hour)",
    );
    await ensureIndex(
      conn,
      "hourly_product_performance_rollup",
      "idx_hpr_date_product",
      "ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_date_product (date, product_id)",
    );

    brand._tablesEnsured = true;
  } finally {
    conn.release();
  }
}

// ---------- Metadata helpers ----------
async function getLastProductSyncDate(brand) {
  const conn = await brand.pool.getConnection();
  try {
    const [rows] = await conn.query(
      `SELECT key_value FROM pipeline_metadata WHERE key_name='last_product_sync_date'`,
    );
    if (!rows.length) return null;
    return rows[0].key_value;
  } finally {
    conn.release();
  }
}

async function setLastProductSyncDate(brand, dateStr) {
  const conn = await brand.pool.getConnection();
  try {
    await conn.query(
      `
      INSERT INTO pipeline_metadata (key_name, key_value)
      VALUES ('last_product_sync_date', ?)
      ON DUPLICATE KEY UPDATE key_value = VALUES(key_value)
    `,
      [dateStr],
    );
  } finally {
    conn.release();
  }
}

// ---------- Product sync (patched pagination) ----------
async function syncProductsForBrand(brand) {
  console.log(`[${brand.tag}] Running daily product sync...`);

  const conn = await brand.pool.getConnection();
  try {
    let url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/products.json?limit=250&fields=id,title,status,handle,product_type`;
    let page = 1;
    let total = 0;

    while (url) {
      console.log(`[${brand.tag}] Fetching products page ${page}...`);

      const resp = await axios.get(url, {
        headers: {
          "X-Shopify-Access-Token": brand.accessToken,
          "Content-Type": "application/json",
        },
        validateStatus: () => true,
      });

      if (resp.status === 429) {
        const retry = Number(resp.headers["retry-after"] || "3");
        console.log(`[${brand.tag}] Rate-limited, sleeping ${retry}s`);
        await new Promise((r) => setTimeout(r, retry * 1000));
        continue;
      }

      if (resp.status !== 200) {
        console.error(
          `[${brand.tag}] Product sync failed:`,
          resp.status,
          resp.data,
        );
        break;
      }

      const products = resp.data.products || [];
      if (!products.length) break;

      const rows = products.map((p) => [
        p.id,
        `/products/${p.handle}`,
        p.status || null,
        p.title || null,
        p.product_type || null,
      ]);

      if (rows.length) {
        const placeholders = rows.map(() => "(?, ?, ?, ?, ?)").join(", ");
        await conn.query(
          `
          INSERT INTO product_landing_mapping (product_id, landing_page_path, status, title, product_type)
          VALUES ${placeholders}
          ON DUPLICATE KEY UPDATE
            product_id=VALUES(product_id),
            status=VALUES(status),
            title=VALUES(title),
            product_type=VALUES(product_type),
            last_synced_at=CURRENT_TIMESTAMP
        `,
          rows.flat(),
        );
        total += rows.length;
      }

      const link = resp.headers.link || resp.headers.Link;
      if (!link) {
        url = null;
      } else {
        const nextPart = link.split(",").find((l) => l.includes('rel="next"'));
        if (!nextPart) {
          url = null;
        } else {
          const matchUrl = nextPart.match(/<([^>]+)>/);
          url = matchUrl ? matchUrl[1] : null;
        }
      }

      page++;
    }

    console.log(
      `[${brand.tag}] Product sync completed. Upserted ~${total} rows.`,
    );
  } catch (err) {
    console.error(`[${brand.tag}] Product sync failed:`, err);
  } finally {
    conn.release();
  }
}

// ---------- ShopifyQL helpers ----------
// (formatShopifyQLTable, buildDayClause, ShopifyQLFetchError now live in
// shopifyql_utils.js, shared with product_type_sales.js)

function buildShopifyQLQuery(
  brand,
  targetYmd = null,
  { limit = 1000, offset = 0 } = {},
) {
  const dayClause = buildDayClause(targetYmd, brand.shopifyQlTimezone);
  const tzClause = `WITH TIMEZONE '${brand.shopifyQlTimezone}'`;
  const offsetClause = offset > 0 ? ` OFFSET ${offset}` : "";

  // ORDER BY must be a *total, stable* order across page requests, otherwise
  // OFFSET pagination can skip or repeat rows tied on `sessions`. Sorting by
  // every GROUP BY dimension after `sessions` makes each grouped row uniquely
  // positioned, so paging is exact for a static day. (Repeats would be harmless
  // anyway — the snapshot's UNIQUE(date, dim_hash) upsert overwrites — but gaps
  // would silently undercount, which is the bug we're fixing.)
  return `
    FROM sessions
      SHOW
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions
      WHERE landing_page_path IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name
      ${tzClause}
      ${dayClause}
      ORDER BY sessions DESC,
        landing_page_type ASC,
        landing_page_path ASC,
        utm_source ASC,
        utm_medium ASC,
        utm_campaign ASC,
        utm_content ASC,
        utm_term ASC,
        referrer_name ASC
      LIMIT ${limit}${offsetClause}
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

// Paginates the daily sessions query with LIMIT/OFFSET, accumulating every page.
// Returns [] for a legitimately empty day; THROWS ShopifyQLFetchError when a page
// can't be fetched after its retries or when pagination runs past the page cap,
// so the caller skips the whole day rather than persist a partial snapshot.
async function fetchShopifyQLSessions(brand, targetYmd = null) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/graphql.json`;
  const pageSize = SHOPIFYQL_PAGE_SIZE;
  const MAX_RETRIES = SHOPIFYQL_FETCH_MAX_RETRIES;
  const allRows = [];

  for (let page = 0; page < SHOPIFYQL_MAX_PAGES; page++) {
    const offset = page * pageSize;
    const q = buildShopifyQLQuery(brand, targetYmd, {
      limit: pageSize,
      offset,
    }).replace(/"/g, '\\"');
    const graphql = {
      query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
    };

    let pageRows = null;
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
          `[${brand.tag}] ShopifyQL page ${page} (offset ${offset}) rate-limited (429), sleeping ${retry}s (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(retry * 1000);
        continue; // 429 is not a wasted attempt
      }

      if (resp.status >= 500) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL page ${page} got ${resp.status}, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (resp.status !== 200) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.error(
          `[${brand.tag}] ShopifyQL page ${page} failed: HTTP ${resp.status} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (resp.data?.errors?.length) {
        const throttleSleepMs = getThrottleResetSleepMsFromGraphQLErrors(
          resp.data.errors,
        );
        if (throttleSleepMs != null) {
          console.warn(
            `[${brand.tag}] ShopifyQL page ${page} THROTTLED, sleeping ${Math.ceil(throttleSleepMs / 1000)}s until window reset (attempt ${attempt}/${MAX_RETRIES})`,
          );
          await sleep(throttleSleepMs);
          continue; // Shopify told us exactly when to retry; don't burn an attempt
        }

        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL page ${page} GraphQL errors (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(resp.data.errors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      const res = resp.data.data?.shopifyqlQuery;
      if (!res) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL page ${page} missing shopifyqlQuery payload (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (res.parseErrors?.length) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL page ${page} parseErrors, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(res.parseErrors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      pageRows = formatShopifyQLTable(res.tableData);
      break;
    }

    if (pageRows == null) {
      // Exhausted retries on this page. Surface a hard failure so the caller
      // skips the entire day — upserting the partial set we have would let the
      // snapshot stale-sweep DELETE previously-good tuples from later pages.
      throw new ShopifyQLFetchError(
        `[${brand.tag}] ShopifyQL page ${page} (offset ${offset}) failed after ${MAX_RETRIES} attempts for ${targetYmd || "today"}.`,
      );
    }

    allRows.push(...pageRows);

    if (pageRows.length < pageSize) {
      // Short read => this was the last page; we have the complete day.
      console.log(
        `[${brand.tag}] ShopifyQL fetched ${allRows.length} rows across ${page + 1} page(s).`,
      );
      return allRows;
    }

    // Full page => there may be more. Small delay to be gentle on rate limits.
    await sleep(200);
  }

  // Never got a short read within the page cap. Treat as incomplete and skip the
  // day rather than persist a truncated snapshot.
  throw new ShopifyQLFetchError(
    `[${brand.tag}] ShopifyQL exceeded max pages (${SHOPIFYQL_MAX_PAGES} x ${pageSize}) for ${targetYmd || "today"}; aborting day to avoid a truncated snapshot.`,
  );
}

// ---------- Hourly ShopifyQL (hour_of_day) ----------
function buildShopifyQLHourlyQuery(brand, targetYmd = null) {
  const dayClause = buildDayClause(targetYmd, brand.shopifyQlTimezone);
  const tzClause = `WITH TIMEZONE '${brand.shopifyQlTimezone}'`;

  return `
    FROM sessions
      SHOW
        hour_of_day,
        sessions,
        sessions_with_cart_additions
      WHERE human_or_bot_session IN ('human', 'bot')
      GROUP BY hour_of_day
      ${tzClause}
      ${dayClause}
      ORDER BY hour_of_day ASC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

async function fetchShopifyQLHourlySessions(brand, targetYmd = null) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/graphql.json`;
  const q = buildShopifyQLHourlyQuery(brand, targetYmd).replace(/"/g, '\\"');

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
      console.log(
        `[${brand.tag}] ShopifyQL hourly rate-limited, sleeping ${retry}s`,
      );
      await new Promise((r) => setTimeout(r, retry * 1000));
      continue;
    }

    if (resp.status !== 200 || resp.data.errors) return [];

    const res = resp.data.data?.shopifyqlQuery;
    if (!res || res.parseErrors?.length) return [];

    return formatShopifyQLTable(res.tableData);
  }
}

// ---------- Hourly Dimension ShopifyQL (per-hour time slices with full dimensions) ----------
function buildHourClause(targetYmd, hour) {
  const hh = String(hour).padStart(2, "0");
  return `SINCE ${targetYmd}T${hh}:00:00 UNTIL ${targetYmd}T${hh}:59:59`;
}

function buildShopifyQLHourlyDimensionAllHoursQuery(brand, targetYmd) {
  const dayClause = buildDayClause(targetYmd, brand.shopifyQlTimezone);
  const tzClause = `WITH TIMEZONE '${brand.shopifyQlTimezone}'`;

  return `
    FROM sessions
      SHOW
        hour_of_day,
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions
      WHERE landing_page_path IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY
        hour_of_day,
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name
      ${tzClause}
      ${dayClause}
      ORDER BY sessions DESC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

function buildShopifyQLHourlyDimensionQuery(brand, targetYmd, hour) {
  const hourClause = buildHourClause(targetYmd, hour);
  const tzClause = `WITH TIMEZONE '${brand.shopifyQlTimezone}'`;

  return `
    FROM sessions
      SHOW
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions
      WHERE landing_page_path IS NOT NULL
        AND human_or_bot_session IN ('human', 'bot')
      GROUP BY
        landing_page_type,
        landing_page_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name
      ${tzClause}
      ${hourClause}
      ORDER BY sessions DESC
      LIMIT 1000
    VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
  `.replace(/\n+/g, " ");
}

async function fetchHourlyDimensionSessions(brand, targetYmd) {
  const url = `https://${brand.shopName}.myshopify.com/admin/api/${brand.apiVersion}/graphql.json`;
  const allRows = [];
  const hourRowCounts = new Map();
  const hourTruncated = new Set();
  const hoursSucceeded = new Set();
  const hoursSkipped = new Set();

  const MAX_RETRIES = Math.max(
    1,
    parseInt(process.env.SHOPIFYQL_HOURLY_DIM_MAX_RETRIES || "5", 10) || 5,
  );

  // Fast path: try a single query that includes hour_of_day in the GROUP BY.
  // If it returns < LIMIT rows, we can use it and avoid 24 separate hour slices.
  try {
    const qAll = buildShopifyQLHourlyDimensionAllHoursQuery(brand, targetYmd).replace(
      /"/g,
      '\\"',
    );
    const graphqlAll = {
      query: `query { shopifyqlQuery(query: "${qAll}") { tableData { rows columns { name } } parseErrors } }`,
    };

    let attempt = 1;
    while (attempt <= MAX_RETRIES) {
      const resp = await axios.post(url, graphqlAll, {
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
          `[${brand.tag}] ShopifyQL hourly-dim(all) rate-limited (429), sleeping ${retry}s (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(retry * 1000);
        continue;
      }

      if (resp.status >= 500) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim(all) got ${resp.status}, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (resp.status !== 200) {
        console.error(
          `[${brand.tag}] ShopifyQL hourly-dim(all) failed: HTTP ${resp.status} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        break;
      }

      if (resp.data?.errors?.length) {
        const throttleSleepMs = getThrottleResetSleepMsFromGraphQLErrors(
          resp.data.errors,
        );
        if (throttleSleepMs != null) {
          console.warn(
            `[${brand.tag}] ShopifyQL hourly-dim(all) THROTTLED, sleeping ${Math.ceil(throttleSleepMs / 1000)}s until window reset (attempt ${attempt}/${MAX_RETRIES})`,
          );
          await sleep(throttleSleepMs);
          // Don't burn an attempt if Shopify told us exactly when to retry.
          continue;
        }

        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim(all) GraphQL errors (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(resp.data.errors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      const res = resp.data.data?.shopifyqlQuery;
      if (!res) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim(all) missing shopifyqlQuery payload (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (res.parseErrors?.length) {
        const sleepMs = Math.min(5000, 500 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim(all) parseErrors, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(res.parseErrors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      const rows = formatShopifyQLTable(res.tableData);
      if (rows.length > 0 && rows.length < 1000) {
        const normalized = [];
        for (const row of rows) {
          const h = Number(row.hour_of_day);
          if (!Number.isFinite(h) || h < 0 || h > 23) continue;
          normalized.push({ ...row, hour: h });
        }

        // Completeness note: single-query path only taken when < 1000 rows.
        const byHour = new Map();
        for (const r of normalized) byHour.set(Number(r.hour), (byHour.get(Number(r.hour)) || 0) + 1);
        for (const [h, c] of byHour.entries()) {
          hourRowCounts.set(h, c);
          hoursSucceeded.add(h);
        }
        console.log(
          `[${brand.tag}] Hourly dimension sessions fetched via single query: ${normalized.length} rows (no LIMIT truncation).`,
        );
        return normalized;
      }

      if (rows.length >= 1000) {
        console.log(
          `[${brand.tag}] Hourly dimension single-query hit LIMIT 1000; falling back to 24 hourly slices.`,
        );
      } else {
        console.log(
          `[${brand.tag}] Hourly dimension single-query returned ${rows.length} rows; falling back to 24 hourly slices.`,
        );
      }

      break;
    }
  } catch (err) {
    console.warn(
      `[${brand.tag}] Hourly dimension single-query attempt failed; falling back to 24 hourly slices: ${err?.message}`,
    );
  }

  for (let h = 0; h < 24; h++) {
    const q = buildShopifyQLHourlyDimensionQuery(brand, targetYmd, h).replace(/"/g, '\\"');
    const graphql = {
      query: `query { shopifyqlQuery(query: "${q}") { tableData { rows columns { name } } parseErrors } }`,
    };

    let succeeded = false;
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
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} rate-limited (429), sleeping ${retry}s (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(retry * 1000);
        continue;
      }

      if (resp.status >= 500) {
        const sleepMs = Math.min(3000, 300 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} got ${resp.status}, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (resp.status !== 200) {
        console.error(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} failed: HTTP ${resp.status} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        break;
      }

      if (resp.data?.errors?.length) {
        const throttleSleepMs = getThrottleResetSleepMsFromGraphQLErrors(
          resp.data.errors,
        );
        if (throttleSleepMs != null) {
          console.warn(
            `[${brand.tag}] ShopifyQL hourly-dim h=${h} THROTTLED, sleeping ${Math.ceil(throttleSleepMs / 1000)}s until window reset (attempt ${attempt}/${MAX_RETRIES})`,
          );
          await sleep(throttleSleepMs);
          // Don't burn an attempt if Shopify told us exactly when to retry.
          continue;
        }

        const sleepMs = Math.min(3000, 300 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} GraphQL errors (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(resp.data.errors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      const res = resp.data.data?.shopifyqlQuery;
      if (!res) {
        const sleepMs = Math.min(3000, 300 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} missing shopifyqlQuery payload (HTTP 200), retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES})`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      if (res.parseErrors?.length) {
        const sleepMs = Math.min(3000, 300 * attempt);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} parseErrors, retrying in ${sleepMs}ms (attempt ${attempt}/${MAX_RETRIES}): ${JSON.stringify(res.parseErrors)}`,
        );
        await sleep(sleepMs);
        attempt += 1;
        continue;
      }

      const rows = formatShopifyQLTable(res.tableData);
      for (const row of rows) row.hour = h;
      allRows.push(...rows);
      hourRowCounts.set(h, rows.length);
      hoursSucceeded.add(h);
      if (rows.length >= 1000) {
        hourTruncated.add(h);
        console.warn(
          `[${brand.tag}] ShopifyQL hourly-dim h=${h} returned ${rows.length} rows (LIMIT 1000). This hour may be truncated.`,
        );
      }
      succeeded = true;
      break;
    }

    if (!succeeded) {
      console.error(
        `[${brand.tag}] ShopifyQL hourly-dim h=${h} failed after ${MAX_RETRIES} attempts; skipping this hour slice.`,
      );
      hoursSkipped.add(h);
    }

    // Small delay between calls to be respectful to rate limits
    if (h < 23) await sleep(200);
  }

  const succeededCount = hoursSucceeded.size;
  const skippedCount = hoursSkipped.size;
  const truncatedCount = hourTruncated.size;

  console.log(
    `[${brand.tag}] Hourly dimension sessions fetched: ${allRows.length} total rows (hours_succeeded=${succeededCount}/24, hours_skipped=${skippedCount}).`,
  );
  if (skippedCount > 0) {
    console.warn(
      `[${brand.tag}] Hourly dimension fetch was PARTIAL for ${targetYmd}. Skipped hours: ${Array.from(hoursSkipped)
        .sort((a, b) => a - b)
        .join(", ")}`,
    );
  }
  if (truncatedCount > 0) {
    console.warn(
      `[${brand.tag}] Hourly dimension fetch may be TRUNCATED for ${targetYmd}. Hours with 1000-row slices: ${Array.from(hourTruncated)
        .sort((a, b) => a - b)
        .join(", ")}`,
    );
  }
  return allRows;
}

// ---------- Snapshot + MV refresh ----------
async function upsertProductSessionsSnapshot(brand, rows, targetYmd) {
  const conn = await brand.pool.getConnection();

  try {
    if (!rows.length) {
      console.log(`[${brand.name}] No snapshot rows for ${targetYmd}.`);
      return;
    }

    // DB-side clock so the stale-sweep cutoff is immune to app/DB skew.
    const [[{ run_started_at: runStartedAt }]] = await conn.query(
      `SELECT NOW() AS run_started_at`,
    );

    const insertRows = rows.map((r) => [
      targetYmd,
      r.landing_page_type || "Unknown",
      r.landing_page_path || null,
      null,
      r.utm_source || null,
      r.utm_medium || null,
      r.utm_campaign || null,
      r.utm_content || null,
      r.utm_term || null,
      r.referrer_name || null,
      Number(r.sessions || 0),
      Number(r.sessions_with_cart_additions || 0),
    ]);

    const placeholders = insertRows
      .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .join(", ");

    await conn.query(
      `
      INSERT INTO product_sessions_snapshot
      (
        date,
        landing_page_type,
        landing_page_path,
        product_title,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        referrer_name,
        sessions,
        sessions_with_cart_additions
      )
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        updated_at                   = CURRENT_TIMESTAMP
    `,
      insertRows.flat(),
    );

    await conn.query(
      `
      UPDATE product_sessions_snapshot s
      JOIN product_landing_mapping m
        ON s.landing_page_path_norm = m.landing_page_path_norm
      SET s.product_id = m.product_id,
          s.product_title = m.title
      WHERE s.date = ?
        AND (s.product_id IS NULL OR s.product_title IS NULL)
        AND (m.product_id IS NOT NULL OR m.title IS NOT NULL)
    `,
      [targetYmd],
    );

    // Sweep tuples that existed for this date but weren't present in this
    // run (e.g. fell out of ShopifyQL's LIMIT 1000 window).
    const [sweep] = await conn.query(
      `DELETE FROM product_sessions_snapshot WHERE date = ? AND updated_at < ?`,
      [targetYmd, runStartedAt],
    );

    console.log(
      `[${brand.name}] Upserted ${insertRows.length} snapshot rows for ${targetYmd}; swept ${sweep.affectedRows} stale.`,
    );
  } finally {
    conn.release();
  }
}

async function refreshMaterializedViews(brand, targetYmd) {
  const conn = await brand.pool.getConnection();

  try {
    // MVs are upserted directly via ON DUPLICATE KEY UPDATE — no prior DELETE
    // needed. The PK of each MV matches the aggregation's GROUP BY, so a
    // tuple that's already present just gets its counters overwritten.

    // ---------- TYPE MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_type_daily
      (date, landing_page_type, product_type, sessions, sessions_with_cart_additions, add_to_cart_rate)
      SELECT
        s.date,
        s.landing_page_type,
        COALESCE(m.product_type, 'Unknown') AS product_type,
        SUM(s.sessions) AS sessions,
        SUM(s.sessions_with_cart_additions) AS sessions_with_cart_additions,
        CASE WHEN SUM(s.sessions) > 0
             THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4)
             ELSE 0 END AS add_to_cart_rate
      FROM product_sessions_snapshot s
      LEFT JOIN product_landing_mapping m
        ON s.landing_page_path_norm = m.landing_page_path_norm
      WHERE s.date = ?
      GROUP BY s.date, s.landing_page_type, product_type
      ON DUPLICATE KEY UPDATE
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        add_to_cart_rate             = VALUES(add_to_cart_rate)
    `,
      [targetYmd],
    );

    // ---------- PATH MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_path_daily
      (
        date,
        landing_page_path,
        product_id,
        sessions,
        sessions_with_cart_additions,
        add_to_cart_rate,
        add_to_cart_rate_pct,
        conversion_rate_pct
      )
      SELECT
        s.date,
        s.landing_page_path,
        m.product_id,
        SUM(s.sessions) AS sessions,
        SUM(s.sessions_with_cart_additions) AS sessions_with_cart_additions,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4)
          ELSE 0
        END AS add_to_cart_rate,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        CASE
          WHEN SUM(s.sessions) > 0
            -- Use MAX(o.orders) to strictly comply with only_full_group_by since o.orders is aggregate per product
            THEN ROUND(COALESCE(MAX(o.orders), 0) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS conversion_rate_pct

      FROM product_sessions_snapshot s

      LEFT JOIN product_landing_mapping m
        ON s.landing_page_path_norm = m.landing_page_path_norm

      LEFT JOIN (
        SELECT
          product_id,
          created_dt AS created_date,
          COUNT(DISTINCT order_id) AS orders
        FROM shopify_orders
        WHERE created_dt = ?
          AND product_id IS NOT NULL
        GROUP BY product_id, created_dt
      ) o
        ON o.product_id   = m.product_id
       AND o.created_date = s.date

      WHERE s.date = ?

      GROUP BY
        s.date,
        s.landing_page_path,
        m.product_id
      ON DUPLICATE KEY UPDATE
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        add_to_cart_rate             = VALUES(add_to_cart_rate),
        add_to_cart_rate_pct         = VALUES(add_to_cart_rate_pct),
        conversion_rate_pct          = VALUES(conversion_rate_pct)
    `,
      [targetYmd, targetYmd],
    );

    // ---------- CAMPAIGN MV ----------
    await conn.query(
      `
      INSERT INTO mv_product_sessions_by_campaign_daily
      (
        date,
        landing_page_path,
        utm_campaign,
        product_id,
        referrer_name,
        sessions,
        sessions_with_cart_additions,
        add_to_cart_rate_pct,
        conversion_rate_pct
      )
      SELECT
        s.date,
        s.landing_page_path,
        COALESCE(s.utm_campaign, '(none)') AS utm_campaign,
        m.product_id,
        s.referrer_name,
        SUM(s.sessions) AS sessions,
        SUM(s.sessions_with_cart_additions) AS sessions_with_cart_additions,

        CASE
          WHEN SUM(s.sessions) > 0
            THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS add_to_cart_rate_pct,

        CASE
          WHEN SUM(s.sessions) > 0
             -- Use MAX(o.orders) for strict group by compliance
            THEN ROUND(COALESCE(MAX(o.orders), 0) / SUM(s.sessions) * 100, 4)
          ELSE 0
        END AS conversion_rate_pct

      FROM product_sessions_snapshot s

      LEFT JOIN product_landing_mapping m
        ON s.landing_page_path_norm = m.landing_page_path_norm

      LEFT JOIN (
        SELECT
          product_id,
          created_dt AS created_date,
          COUNT(DISTINCT order_id) AS orders
        FROM shopify_orders
        WHERE created_dt = ?
          AND product_id IS NOT NULL
        GROUP BY product_id, created_dt
      ) o
        ON o.product_id   = m.product_id
       AND o.created_date = s.date

      WHERE
        s.date = ?
        AND s.utm_campaign IS NOT NULL

      GROUP BY
        s.date,
        s.landing_page_path,
        utm_campaign,
        m.product_id,
        s.referrer_name
      ON DUPLICATE KEY UPDATE
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        add_to_cart_rate_pct         = VALUES(add_to_cart_rate_pct),
        conversion_rate_pct          = VALUES(conversion_rate_pct)
    `,
      [targetYmd, targetYmd],
    );

    console.log(
      `[${brand.name}] Refreshed MVs for ${targetYmd} (upserted).`,
    );
  } catch (err) {
    console.error(`[${brand.name}] MV Refresh Error:`, err);
    throw err;
  } finally {
    conn.release();
  }
}

// ---------- Hourly summary upsert ----------
async function upsertHourlySessionsSummary(brand, hourlyRows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    const byHour = new Map();
    for (const r of hourlyRows) {
      const h = Number(r.hour_of_day);
      if (!Number.isFinite(h) || h < 0 || h > 23) continue;
      byHour.set(h, {
        sessions: Number(r.sessions || 0),
        atc: Number(r.sessions_with_cart_additions || 0),
      });
    }

    const insertRows = [];
    for (let h = 0; h < 24; h++) {
      const v = byHour.get(h) || { sessions: 0, atc: 0 };
      insertRows.push([targetYmd, h, v.sessions, v.atc]);
    }

    const placeholders = insertRows.map(() => "(?, ?, ?, ?)").join(", ");
    await conn.query(
      `
      INSERT INTO hourly_sessions_summary_shopify
        (date, hour, number_of_sessions, number_of_atc_sessions)
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE
        number_of_sessions     = VALUES(number_of_sessions),
        number_of_atc_sessions = VALUES(number_of_atc_sessions)
      `,
      insertRows.flat(),
    );

    console.log(
      `[${brand.name}] Hourly sessions summary upserted for ${targetYmd}`,
    );
  } finally {
    conn.release();
  }
}

// ---------- Hourly product sessions upsert (full dimensions) ----------
async function upsertHourlyProductSessions(brand, rows, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    if (!rows.length) {
      console.log(
        `[${brand.name}] No hourly product session rows for ${targetYmd}.`,
      );
      return;
    }

    const [[{ run_started_at: runStartedAt }]] = await conn.query(
      `SELECT NOW() AS run_started_at`,
    );

    // Batch upsert in chunks of 500 to avoid packet size issues.
    const CHUNK = 500;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const insertRows = chunk.map((r) => [
        targetYmd,
        Number(r.hour),
        r.landing_page_type || "Unknown",
        r.landing_page_path || null,
        r.utm_source || null,
        r.utm_medium || null,
        r.utm_campaign || null,
        r.utm_content || null,
        r.utm_term || null,
        r.referrer_name || null,
        Number(r.sessions || 0),
        Number(r.sessions_with_cart_additions || 0),
      ]);

      const placeholders = insertRows
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .join(", ");

      await conn.query(
        `
        INSERT INTO hourly_product_sessions
        (
          date, hour, landing_page_type, landing_page_path,
          utm_source, utm_medium, utm_campaign, utm_content, utm_term,
          referrer_name, sessions, sessions_with_cart_additions
        )
        VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE
          sessions                     = VALUES(sessions),
          sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
          updated_at                   = CURRENT_TIMESTAMP
        `,
        insertRows.flat(),
      );
    }

    // Backfill product_id + product_title from product_landing_mapping
    await conn.query(
      `
      UPDATE hourly_product_sessions h
      JOIN product_landing_mapping m
        ON h.landing_page_path_norm = m.landing_page_path_norm
      SET h.product_id = m.product_id,
          h.product_title = m.title
      WHERE h.date = ?
        AND (h.product_id IS NULL OR h.product_title IS NULL)
        AND (m.product_id IS NOT NULL OR m.title IS NOT NULL)
      `,
      [targetYmd],
    );

    // Sweep tuples that existed for this date but weren't present in this run.
    const [sweep] = await conn.query(
      `DELETE FROM hourly_product_sessions WHERE date = ? AND updated_at < ?`,
      [targetYmd, runStartedAt],
    );

    console.log(
      `[${brand.name}] Upserted ${rows.length} hourly product session rows for ${targetYmd}; swept ${sweep.affectedRows} stale.`,
    );
  } finally {
    conn.release();
  }
}

async function upsertHourlyProductPerformanceRollup(brand, targetYmd) {
  const conn = await brand.pool.getConnection();
  try {
    const [[{ run_started_at: runStartedAt }]] = await conn.query(
      `SELECT NOW() AS run_started_at`,
    );

    const [result] = await conn.query(
      `
      INSERT INTO hourly_product_performance_rollup
      (
        date,
        hour,
        product_id,
        product_title,
        sessions,
        sessions_with_cart_additions,
        orders,
        units_sold,
        total_sales,
        add_to_cart_rate,
        cvr
      )
      SELECT
        k.date,
        k.hour,
        k.product_id,
        COALESCE(s.product_title, o.product_title, 'Unknown') AS product_title,
        COALESCE(s.sessions, 0) AS sessions,
        COALESCE(s.sessions_with_cart_additions, 0) AS sessions_with_cart_additions,
        COALESCE(o.orders, 0) AS orders,
        COALESCE(o.units_sold, 0) AS units_sold,
        COALESCE(o.total_sales, 0.00) AS total_sales,
        ROUND(
          COALESCE(s.sessions_with_cart_additions, 0) / NULLIF(COALESCE(s.sessions, 0), 0),
          4
        ) AS add_to_cart_rate,
        ROUND(
          COALESCE(o.orders, 0) / NULLIF(COALESCE(s.sessions, 0), 0),
          4
        ) AS cvr
      FROM (
        SELECT date, hour, product_id
        FROM (
          SELECT
            h.date,
            h.hour,
            h.product_id
          FROM hourly_product_sessions h
          WHERE h.date = ?
            AND h.product_id IS NOT NULL
            AND h.product_id <> ''
          GROUP BY h.date, h.hour, h.product_id

          UNION

          SELECT
            o.created_dt AS date,
            o.created_hr AS hour,
            o.product_id
          FROM shopify_orders o
          WHERE o.created_dt = ?
            AND o.order_id IS NOT NULL
            AND o.product_id IS NOT NULL
            AND o.product_id <> ''
          GROUP BY o.created_dt, o.created_hr, o.product_id
        ) keys_union
      ) k
      LEFT JOIN (
        SELECT
          h.date,
          h.hour,
          h.product_id,
          MAX(h.product_title) AS product_title,
          SUM(h.sessions) AS sessions,
          SUM(h.sessions_with_cart_additions) AS sessions_with_cart_additions
        FROM hourly_product_sessions h
        WHERE h.date = ?
          AND h.product_id IS NOT NULL
          AND h.product_id <> ''
        GROUP BY h.date, h.hour, h.product_id
      ) s
        ON s.date = k.date
       AND s.hour = k.hour
       AND s.product_id = k.product_id
      LEFT JOIN (
        SELECT
          o.created_dt AS date,
          o.created_hr AS hour,
          o.product_id,
          MAX(o.line_item) AS product_title,
          COUNT(DISTINCT o.order_id) AS orders,
          SUM(GREATEST(COALESCE(o.line_item_quantity, 0), 0)) AS units_sold,
          ROUND(
            SUM(
              (COALESCE(o.line_item_price, 0) * GREATEST(COALESCE(o.line_item_quantity, 0), 0))
              - COALESCE(o.line_item_total_discount, 0)
            ),
            2
          ) AS total_sales
        FROM shopify_orders o
        WHERE o.created_dt = ?
          AND o.order_id IS NOT NULL
          AND o.product_id IS NOT NULL
          AND o.product_id <> ''
        GROUP BY o.created_dt, o.created_hr, o.product_id
      ) o
        ON o.date = k.date
       AND o.hour = k.hour
       AND o.product_id = k.product_id
      ON DUPLICATE KEY UPDATE
        product_title                = VALUES(product_title),
        sessions                     = VALUES(sessions),
        sessions_with_cart_additions = VALUES(sessions_with_cart_additions),
        orders                       = VALUES(orders),
        units_sold                   = VALUES(units_sold),
        total_sales                  = VALUES(total_sales),
        add_to_cart_rate             = VALUES(add_to_cart_rate),
        cvr                          = VALUES(cvr),
        updated_at                   = CURRENT_TIMESTAMP
      `,
      [targetYmd, targetYmd, targetYmd, targetYmd],
    );

    const [sweep] = await conn.query(
      `DELETE FROM hourly_product_performance_rollup WHERE date = ? AND updated_at < ?`,
      [targetYmd, runStartedAt],
    );

    console.log(
      `[${brand.name}] Product performance rollup upserted for ${targetYmd}. Rows: ${result.affectedRows || 0}; swept ${sweep.affectedRows} stale.`,
    );
  } finally {
    conn.release();
  }
}

// ---------- Pipeline per brand (date-aware) ----------
async function processBrand(brand, requestedTargetYmd) {
  const targetYmd = requestedTargetYmd || todayYMDInTimeZone(brand.shopifyQlTimezone);
  const startTotal = Date.now();
  console.log(
    `\n========== ${brand.tag} (${targetYmd}, tz=${brand.shopifyQlTimezone}) START ==========\n`,
  );

  await ensureTablesForBrand(brand);

  // Product sync once per real brand-local day (not per backfill date)
  const realToday = todayYMDInTimeZone(brand.shopifyQlTimezone);
  const lastSync = await getLastProductSyncDate(brand);

  if (lastSync !== realToday) {
    await syncProductsForBrand(brand);
    await setLastProductSyncDate(brand, realToday);
  } else {
    console.log(
      `[${brand.tag}] Product sync already done today (${realToday}), skipping.`,
    );
  }

  // Main sessions snapshot + MVs.
  // The daily fetch paginates via LIMIT/OFFSET. If a page exhausts its retries
  // (or pagination blows past the page cap) it throws ShopifyQLFetchError, and
  // we skip the ENTIRE day for this brand — snapshot, MVs, and the hourly steps
  // below are all left untouched, preserving the previous good data. Caught here
  // (not propagated) so one brand's bad day can't take down the others.
  let rows;
  try {
    rows = await fetchShopifyQLSessions(brand, targetYmd);
  } catch (err) {
    if (err instanceof ShopifyQLFetchError) {
      console.error(
        `[${brand.tag}] Skipping ${targetYmd} entirely (snapshot, MVs, and hourly steps left unchanged): ${err.message}`,
      );
      return;
    }
    throw err;
  }
  await upsertProductSessionsSnapshot(brand, rows, targetYmd);
  await refreshMaterializedViews(brand, targetYmd);

  // Product-type sales/orders (from Shopify's `sales` ShopifyQL report) +
  // funnel rollup joining it with the session-by-type MV just refreshed above.
  // Additive metric — a failure here is logged and skipped, not fatal to the
  // rest of the day (unlike the main sessions fetch above).
  await ensureProductTypeSalesTables(brand);
  try {
    const salesRows = await fetchProductTypeSales(brand, targetYmd);
    await upsertProductTypeSalesSnapshot(brand, salesRows, targetYmd);
    await refreshProductTypeFunnelRollup(brand, targetYmd);
  } catch (err) {
    if (err instanceof ShopifyQLFetchError) {
      console.error(
        `[${brand.tag}] Skipping product-type sales for ${targetYmd}: ${err.message}`,
      );
    } else {
      throw err;
    }
  }

  // Hourly summary
  const hourly = await fetchShopifyQLHourlySessions(brand, targetYmd);
  await upsertHourlySessionsSummary(brand, hourly, targetYmd);

  // Hourly product sessions with full dimensions (feature-flagged)
  if (
    HOURLY_PRODUCT_SESSIONS_ENABLED &&
    (!BACKFILL_MODE || HOURLY_PRODUCT_SESSIONS_BACKFILL_ENABLED)
  ) {
    const hourlyDim = await fetchHourlyDimensionSessions(brand, targetYmd);
    await upsertHourlyProductSessions(brand, hourlyDim, targetYmd);
    await upsertHourlyProductPerformanceRollup(brand, targetYmd);
  }

  console.log(
    `[${brand.tag}] Pipeline complete for ${targetYmd}. Duration: ${(Date.now() - startTotal) / 1000}s`,
  );
}

// ---------- Runners ----------
async function runPipelineForDate(targetYmd) {
  const brands = await getBrands();
  console.log(
    `\n🚀 Product Sessions Pipeline @ ${fmtIST()} (target=${targetYmd || "brand-local today"}) | brand concurrency=${HOURLY_BRAND_CONCURRENCY}/${brands.length}\n`,
  );
  await mapWithConcurrency(brands, HOURLY_BRAND_CONCURRENCY, (b) =>
    processBrand(b, targetYmd),
  );
  console.log(`\n✅ All brands completed for ${targetYmd || "brand-local today"}.\n`);
}

async function runBackfillPipeline() {
  const brands = selectBackfillBrands(await getBrands());

  // Rolling mode: each brand refetches its own last N brand-local days, so the
  // window stays correct even when brands sit in different timezones. Brands run
  // concurrently; each walks its own date list sequentially.
  if (BACKFILL_LOOKBACK_DAYS > 0) {
    console.log(
      `\n🧱 Backfill mode enabled @ ${fmtIST()} (rolling: last ${BACKFILL_LOOKBACK_DAYS} brand-local day(s) per brand)\n`,
    );
    await mapWithConcurrency(brands, BACKFILL_BRAND_CONCURRENCY, async (brand) => {
      const dates = recentDatesForTimeZone(
        brand.shopifyQlTimezone,
        BACKFILL_LOOKBACK_DAYS,
      );
      console.log(
        `[BACKFILL] ${brand.tag} (tz=${brand.shopifyQlTimezone}): ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} day(s))`,
      );
      for (const d of dates) {
        await processBrand(brand, d);
      }
    });
    console.log(
      `\n🏁 Rolling backfill complete (last ${BACKFILL_LOOKBACK_DAYS} day(s) per brand).\n`,
    );
    return;
  }

  // Explicit historical range: one shared label list handed to every brand.
  if (!BACKFILL_START_IST_DATE || !BACKFILL_END_IST_DATE) {
    throw new Error(
      `[BACKFILL] BACKFILL_MODE=true requires BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE (or set BACKFILL_LOOKBACK_DAYS for a rolling window)`,
    );
  }

  const dates = buildInclusiveDateRangeYMD(
    BACKFILL_START_IST_DATE,
    BACKFILL_END_IST_DATE,
  );

  console.log(`\n🧱 Backfill mode enabled @ ${fmtIST()}`);
  console.log(
    `[BACKFILL] Range: ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} days)\n`,
  );

  for (const d of dates) {
    console.log(`\n🗓️ [BACKFILL] Running for ${d} ...\n`);
    if (brands.length <= 1 || BACKFILL_BRAND_CONCURRENCY >= brands.length) {
      await Promise.all(brands.map((b) => processBrand(b, d)));
    } else {
      console.log(
        `[BACKFILL] Brand concurrency limit: ${BACKFILL_BRAND_CONCURRENCY} (brands=${brands.length})`,
      );
      await mapWithConcurrency(brands, BACKFILL_BRAND_CONCURRENCY, (b) =>
        processBrand(b, d),
      );
    }
  }

  console.log(`\n🏁 Backfill complete for ${dates.length} days.\n`);
}

// ---------- Scheduler wrapper ----------
let running = false;

async function safeRun(trigger = "unknown", options = {}) {
  if (running) {
    console.log(
      `[SCHED] Skipping run (${trigger}) because previous run is still running.`,
    );
    return;
  }

  running = true;
  const startedAt = fmtIST();
  console.log(`\n[SCHED] Starting pipeline (${trigger}) @ ${startedAt}\n`);

  let runId = null;
  const isManual = trigger === "manual_http";
  
  if (isManual) {
    runId = options.runId;
    const managerStartedEndpoint = process.env.MANAGER_STARTED_ENDPOINT;
    const pipelineId = process.env.PIPELINE_ID;
    
    if (managerStartedEndpoint && pipelineId) {
      try {
        await axios.post(managerStartedEndpoint, {
          runId,
          pipelineId,
          status: "running"
        }, {
          headers: { "x-pipeline-key": PIPELINE_AUTH_HEADER }
        });
        console.log(`[SCHED] Reported 'running' status to manager for runId: ${runId}`);
      } catch (err) {
        console.warn(`[SCHED] Failed to report 'running' status: ${err.message}`);
      }
    } else {
      console.warn(`[SCHED] MANAGER_STARTED_ENDPOINT or PIPELINE_ID not set. Skipping 'running' status report.`);
    }
  }

  try {
    if (BACKFILL_MODE) {
      await runBackfillPipeline();
    } else {
      await runPipelineForDate(null);
    }
    console.log(`\n[SCHED] Pipeline completed (${trigger}) @ ${fmtIST()}\n`);

    if (isManual && runId) {
      const managerCompletedEndpoint = process.env.MANAGER_COMPLETED_ENDPOINT;
      const pipelineId = process.env.PIPELINE_ID;
      
      if (managerCompletedEndpoint && pipelineId) {
        try {
          await axios.post(managerCompletedEndpoint, {
            runId,
            pipelineId,
            status: "completed"
          }, {
            headers: { "x-pipeline-key": PIPELINE_AUTH_HEADER }
          });
          console.log(`[SCHED] Reported 'completed' status to manager for runId: ${runId}`);
        } catch (err) {
          console.warn(`[SCHED] Failed to report 'completed' status: ${err.message}`);
        }
      }
    }
  } catch (err) {
    console.error(`[SCHED] Pipeline crashed (${trigger}) @ ${fmtIST()}:`, err);

    if (isManual && runId) {
      const managerFailedEndpoint = process.env.MANAGER_FAILED_ENDPOINT;
      const pipelineId = process.env.PIPELINE_ID;
      const errorCode = deriveErrorCode(err);
      const errorBody = getLastRuntimeLogs() || (err?.message || String(err));

      if (managerFailedEndpoint && pipelineId) {
        try {
          await axios.post(
            managerFailedEndpoint,
            {
              pipelineId,
              runId,
              errorCode,
              errorBody,
            },
            {
              headers: { "x-pipeline-key": PIPELINE_AUTH_HEADER },
            },
          );
          console.log(
            `[SCHED] Reported 'failed' status to manager for runId: ${runId}`,
          );
        } catch (reportErr) {
          console.warn(
            `[SCHED] Failed to report 'failed' status: ${reportErr.message}`,
          );
        }
      } else {
        console.warn(
          `[SCHED] MANAGER_FAILED_ENDPOINT or PIPELINE_ID not set. Skipping 'failed' status report.`,
        );
      }
    }
  } finally {
    running = false;
  }
}

// ---------- Main ----------
console.log(`[DEBUG] Checking main module...`);
console.log(`[DEBUG] import.meta.url: ${import.meta.url}`);
console.log(`[DEBUG] process.argv[1]: ${process.argv[1]}`);

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (BACKFILL_MODE) {
    // Backfill once on startup; do NOT schedule hourly cron
    if (EXIT_AFTER_BACKFILL) {
      // One-shot mode for CI/cron runners: run the backfill, then exit the
      // process. No HTTP server is started (see guard below). Exit code is 0 on
      // completion — per-brand/per-day failures are logged and skipped, not
      // fatal — and 1 only if the run promise unexpectedly rejects.
      console.log(`[SCHED] Backfill one-shot mode; will exit on completion.`);
      safeRun("backfill_startup")
        .then(() => {
          console.log(
            `[SCHED] Backfill finished; exiting (EXIT_AFTER_BACKFILL) @ ${fmtIST()}`,
          );
          process.exit(0);
        })
        .catch((err) => {
          console.error(`[SCHED] Backfill run rejected unexpectedly:`, err);
          process.exit(1);
        });
    } else {
      safeRun("backfill_startup");
      console.log(`[SCHED] Backfill mode is ON; cron is disabled.`);
      console.log(`[SCHED] Service started @ ${fmtIST()}`);
    }
  } else {
    // 1) Optionally run immediately on deployment/startup.
    //    Default: off — we follow a fixed schedule to avoid colliding with
    //    the other pipeline that runs at minutes 0, 10, 20, ... and draining
    //    RDS EBSByteBalance%. Set RUN_ON_BOOT=true to force an initial run.
    const RUN_ON_BOOT =
      String(process.env.RUN_ON_BOOT || "").toLowerCase().trim() === "true";

    if (RUN_ON_BOOT) {
      safeRun("startup");
    } else {
      console.log(
        `[SCHED] RUN_ON_BOOT is off; waiting for next scheduled tick.`,
      );
    }

    // 2) Cron: run at minute 5 of every hour (Asia/Kolkata) so we don't
    //    collide with the other pipeline's 0/10/20/... schedule.
    if (SELF_RUN) {
      cron.schedule(
        "0 5 * * * *", // second minute hour day month weekday
        () => safeRun("hourly"),
        { timezone: "Asia/Kolkata" },
      );

      console.log(
        `[SCHED] Cron enabled: runs at minute 5 of every hour (Asia/Kolkata).`,
      );
    } else {
      console.log(
        `[SCHED] SELF_RUN is false; automatic hourly schedule disabled.`,
      );
    }
    console.log(`[SCHED] Service started @ ${fmtIST()}`);
  }

  // Generic Manual Triggering.
  // Skipped entirely in one-shot backfill mode — that path runs the backfill and
  // exits, so there's no long-lived process to serve HTTP from.
  if (!(BACKFILL_MODE && EXIT_AFTER_BACKFILL)) {
  const app = express();
  const PORT = process.env.PORT || 8080;
  app.use(express.json());

  function getManualRunId(req) {
    const runId = req.body?.runId;
    return typeof runId === "string" ? runId.trim() : "";
  }

  // Authenticated manual trigger
  app.post("/trigger", (req, res) => {
    const authKey = req.headers["x-pipeline-key"];
    if (authKey !== PIPELINE_AUTH_HEADER) {
      console.warn(`[SERVER] Unauthorized /trigger attempt from IP: ${req.ip}`);
      return res.status(401).json({ error: "Unauthorized" });
    }

    const runId = getManualRunId(req);
    if (!runId) {
      return res.status(400).json({ error: "runId is required" });
    }

    console.log(`[SERVER] /trigger triggered manually.`);
    safeRun("manual_http", { runId });
    res.json({
      status: "triggered",
      runId,
      message: "Pipeline trigger received. Check logs for progress.",
      timestamp: fmtIST(),
    });
  });

  // Legacy endpoint (now authenticated)
  app.post("/run-pipeline", (req, res) => {
    const authKey = req.headers["x-pipeline-key"];
    if (authKey !== PIPELINE_AUTH_HEADER) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const runId = getManualRunId(req);
    if (!runId) {
      return res.status(400).json({ error: "runId is required" });
    }

    safeRun("manual_http", { runId });
    res.json({ status: "triggered", runId, timestamp: fmtIST() });
  });

  app.listen(PORT, () => {
    console.log(`[SERVER] HTTP server listening on port ${PORT}`);
  });
  } // end HTTP-server guard
}
