// Shared fakes for the unit tests. Nothing here talks to a real DB or network.
import { mock } from "node:test";
import crypto from "crypto";

// ---------- Env ----------
// pipeline.js and friends read env into module-level constants at import time,
// so tests call this BEFORE a dynamic `await import(...)` of the module under
// test. DOTENV_CONFIG_PATH points dotenv away from any developer/server .env so
// local config can never leak into a test run.
export function setTestEnv(vars = {}) {
  process.env.DOTENV_CONFIG_PATH = "/nonexistent/.env";
  Object.assign(process.env, vars);
}

// ---------- Fake mysql2 pool/connection ----------
// `handler(sql, params)` returns the value conn.query resolves to (mysql2 shape:
// [rows, fields]). Every call is recorded on pool.calls for assertions.
export function makeFakePool(handler = defaultQueryHandler) {
  const pool = {
    calls: [],
    acquired: 0,
    released: 0,
    async getConnection() {
      pool.acquired += 1;
      return {
        async query(sql, params) {
          pool.calls.push({ sql: String(sql), params });
          return handler(String(sql), params);
        },
        release() {
          pool.released += 1;
        },
      };
    },
  };
  return pool;
}

export function defaultQueryHandler(sql) {
  if (/SELECT NOW\(\) AS run_started_at/.test(sql)) {
    return [[{ run_started_at: "2025-01-01 00:00:00" }], []];
  }
  if (/^\s*(SHOW INDEX|SHOW COLUMNS)/.test(sql)) return [[{}], []];
  if (/^\s*(DELETE|INSERT|UPDATE)/.test(sql)) return [{ affectedRows: 0 }, []];
  return [[], []];
}

export function makeBrand(overrides = {}) {
  return {
    index: 0,
    id: 1,
    tag: "tst",
    name: "TEST",
    shopName: "test-shop",
    apiVersion: "2025-10",
    accessToken: "tok",
    shopifyQlTimezone: "Asia/Kolkata",
    pool: makeFakePool(),
    _tablesEnsured: false,
    ...overrides,
  };
}

// ---------- HTTP response builders (axios with validateStatus: () => true) ----------
export function httpResp(status, data = {}, headers = {}) {
  return { status, data, headers };
}

export function shopifyqlResp(rows, { columns, parseErrors } = {}) {
  return httpResp(200, {
    data: {
      shopifyqlQuery: {
        tableData: { rows, columns: columns || [] },
        parseErrors: parseErrors || [],
      },
    },
  });
}

// Mocks axios.post to return the queued responses in order (last one repeats).
export function queueAxiosPost(axios, responses) {
  const queue = [...responses];
  return mock.method(axios, "post", async () =>
    queue.length > 1 ? queue.shift() : queue[0],
  );
}

// ---------- Fast timers ----------
// Retry loops sleep via setTimeout; collapse every delay to 0 and record the
// requested durations so tests can assert backoff without actually waiting.
export function fastTimers() {
  const delays = [];
  const original = globalThis.setTimeout;
  mock.method(globalThis, "setTimeout", (fn, ms, ...args) => {
    delays.push(ms);
    return original(fn, 0, ...args);
  });
  return delays;
}

export function silenceConsole() {
  for (const level of ["log", "warn", "error"]) {
    mock.method(console, level, () => {});
  }
}

// ---------- AES helper matching pipeline.js decryptAES ----------
export function encryptAES(plain, rawKey) {
  let key = Buffer.from(rawKey, "utf8");
  if (key.length < 32) {
    const padded = Buffer.alloc(32);
    key.copy(padded);
    key = padded;
  } else if (key.length > 32) {
    key = key.subarray(0, 32);
  }
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv("aes-256-cbc", key, iv);
  const enc = Buffer.concat([cipher.update(plain, "utf8"), cipher.final()]);
  return `${iv.toString("base64")}:${enc.toString("base64")}`;
}

// ---------- Brands API + Shopify fakes for orchestration tests ----------
export const BRANDS_API = "https://brands.test/api/brands";

export function apiBrand(id, overrides = {}) {
  return {
    is_active: true,
    shop_name: `shop${id}`,
    api_version: "2025-10",
    access_token: `token${id}`,
    db_host: `db${id}.local`,
    db_user: "u",
    db_password: "pw",
    db_database: `db${id}`,
    brand_tag: `b${id}`,
    brand_name: `Brand${id}`,
    store_timezone: "(GMT+05:30) Asia/Kolkata",
    ...overrides,
  };
}

// Mocks axios.get (brands API + Shopify products REST) and mysql.createPool.
// `brandsById` maps id -> brand detail payload (or an Error to make that fetch
// fail). Returns the created fake pools in creation order; each pool's query
// handler is `handlers.current` so a test can swap DB behaviour mid-run.
export function mockBrandsApi(axios, mysql, brandsById, handlers = { current: defaultQueryHandler }) {
  const pools = [];
  mock.method(axios, "get", async (url) => {
    if (url.includes("myshopify.com")) return httpResp(200, { products: [] });
    if (url === BRANDS_API) {
      return { data: Object.fromEntries(Object.keys(brandsById).map((id) => [id, {}])) };
    }
    const id = url.slice(BRANDS_API.length + 1);
    const detail = brandsById[id];
    if (detail instanceof Error) throw detail;
    return { data: detail };
  });
  mock.method(mysql, "createPool", (config) => {
    const pool = makeFakePool((sql, params) => handlers.current(sql, params));
    pool.config = config;
    pools.push(pool);
    return pool;
  });
  return pools;
}

// One ShopifyQL row shape that satisfies every query in processBrand: the daily
// sessions fetch, hourly summary, hourly-dimension fast path, and the sales
// report (product_type + matching day, taken from the query's SINCE date).
// `onPost(url, body)` may return a response to override the default.
export function mockShopifyQL(axios, { onPost } = {}) {
  return mock.method(axios, "post", async (url, body) => {
    if (onPost) {
      const override = await onPost(url, body);
      if (override) return override;
    }
    if (!url.includes("myshopify.com")) return httpResp(200, {});
    const m = body.query.match(/SINCE (\d{4}-\d{2}-\d{2})T/);
    const day = m ? m[1] : null;
    return shopifyqlResp([
      { hour_of_day: 1, landing_page_type: "Product", landing_page_path: "/products/a", sessions: 4, product_type: "A", day },
    ]);
  });
}

export function sqlLog(pool) {
  return pool.calls.map((c) => c.sql.replace(/\s+/g, " ").trim());
}

// process.exit would kill the test runner; turn it into a catchable throw.
export function trapProcessExit() {
  return mock.method(process, "exit", (code) => {
    throw new Error(`process.exit(${code})`);
  });
}
