// TEST_MODE=true: loadBrands -> loadBrandsFromEnvLocal (.env.local is faked via fs mocks).
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import mysql from "mysql2/promise";
import {
  setTestEnv,
  makeFakePool,
  encryptAES,
  silenceConsole,
  trapProcessExit,
} from "./helpers.js";

const AES_KEY = "env-local-key";
setTestEnv({ TEST_MODE: "true", PASSWORD_AES_KEY: AES_KEY });
delete process.env.DB_SSL_ENABLED;
const p = await import("../pipeline.js");

let pools;
beforeEach(() => {
  silenceConsole();
  pools = [];
  mock.method(mysql, "createPool", (config) => {
    const pool = makeFakePool();
    pool.config = config;
    pools.push(pool);
    return pool;
  });
});
afterEach(() => mock.restoreAll());

function fakeEnvLocal(contents) {
  mock.method(fs, "existsSync", (path) => String(path).endsWith(".env.local") || false);
  mock.method(fs, "readFileSync", (path, enc) => {
    if (String(path).endsWith(".env.local")) return contents;
    throw new Error(`unexpected read ${path}`);
  });
}

const jsonBrand = (overrides = {}) => ({
  brand_id: 9,
  shopName: "s",
  api_version: "2025-10",
  accessToken: "tok",
  dbHost: "x.rds.amazonaws.com",
  db_user: "u",
  dbPassword: "pw",
  db_database: "d",
  brandTag: "t",
  store_timezone: "Europe/London",
  ...overrides,
});

describe("loadBrandsFromEnvLocal", () => {
  test("loadBrands delegates to .env.local in TEST_MODE and accepts camel/snake keys", async () => {
    fakeEnvLocal(`BRANDS_JSON='${JSON.stringify([
      jsonBrand({ accessToken: encryptAES("decrypted", AES_KEY) }),
    ])}'`);
    const brands = await p.loadBrands();
    assert.equal(brands.length, 1);
    const [b] = brands;
    assert.equal(b.id, 9);
    assert.equal(b.tag, "t");
    assert.equal(b.name, "T");
    assert.equal(b.shopName, "s");
    assert.equal(b.accessToken, "decrypted");
    assert.equal(b.shopifyQlTimezone, "Europe/London");
    assert.equal(pools[0].config.password, "pw");
    assert.equal(pools[0].config.ssl, undefined, "TLS never used in TEST_MODE");
  });

  test("falls back to indexed env vars when BRANDS_JSON is absent", async () => {
    fakeEnvLocal(
      [
        "SHOP_NAME_0=s0", "API_VERSION_0=2025-10", "ACCESS_TOKEN_0=t0",
        "DB_HOST_0=h", "DB_USER_0=u", "DB_DATABASE_0=d", "BRAND_TAG_0=zero",
      ].join("\n"),
    );
    const brands = await p.loadBrandsFromEnvLocal();
    assert.deepEqual(brands.map((b) => [b.index, b.id, b.tag, b.shopName]), [[0, 1, "zero", "s0"]]);
  });

  test("skips brands that fail decryption or miss required fields", async () => {
    fakeEnvLocal(`BRANDS_JSON='${JSON.stringify([
      jsonBrand({ accessToken: encryptAES("x", "wrong") }),
      jsonBrand({ shopName: undefined }),
      jsonBrand({ brand_id: "abc", brandTag: undefined }),
    ])}'`);
    const brands = await p.loadBrandsFromEnvLocal();
    assert.equal(brands.length, 1);
    assert.equal(brands[0].id, 1, "non-numeric id falls back to index + 1");
    assert.equal(brands[0].tag, "brand_0");
  });

  for (const [label, setup] of [
    ["file is missing", () => mock.method(fs, "existsSync", () => false)],
    ["file cannot be read", () => {
      mock.method(fs, "existsSync", () => true);
      mock.method(fs, "readFileSync", () => { throw new Error("EACCES"); });
    }],
    ["BRANDS_JSON is invalid", () => fakeEnvLocal("BRANDS_JSON='{not json'")],
    ["no brands are defined", () => fakeEnvLocal("FOO=bar")],
  ]) {
    test(`exits when the ${label}`, async () => {
      const exit = trapProcessExit();
      setup();
      await assert.rejects(p.loadBrandsFromEnvLocal(), /process\.exit\(1\)/);
      assert.equal(exit.mock.callCount(), 1);
    });
  }
});
