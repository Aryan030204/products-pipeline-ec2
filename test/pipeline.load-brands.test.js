// loadBrands (brands API path) and getBrands caching.
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import axios from "axios";
import mysql from "mysql2/promise";
import {
  setTestEnv,
  BRANDS_API,
  apiBrand,
  mockBrandsApi,
  encryptAES,
  silenceConsole,
  trapProcessExit,
} from "./helpers.js";

const AES_KEY = "brands-key";
setTestEnv({
  GET_BRANDS_API: BRANDS_API,
  PIPELINE_AUTH_HEADER: "secret-header",
  PASSWORD_AES_KEY: AES_KEY,
  SHOPIFYQL_TIMEZONE: "Asia/Kolkata",
});
delete process.env.DB_SSL_ENABLED;
delete process.env.DB_SSL_CA;
delete process.env.DB_SSL_CA_FILE;
const p = await import("../pipeline.js");

beforeEach(() => silenceConsole());
afterEach(() => {
  mock.restoreAll();
  delete process.env.DB_SSL_ENABLED;
  delete process.env.DB_SSL_CA;
});

describe("loadBrands (API)", () => {
  test("loads active brands with decrypted credentials and auth header", async () => {
    const pools = mockBrandsApi(axios, mysql, {
      1: apiBrand(1, { access_token: encryptAES("shpat_1", AES_KEY), db_password: encryptAES("dbpw", AES_KEY) }),
      2: apiBrand(2, { brand_tag: undefined, brand_name: undefined, store_timezone: "garbage" }),
    });
    const brands = await p.loadBrands();

    assert.equal(brands.length, 2);
    const [b1, b2] = brands;
    assert.equal(b1.index, 0);
    assert.equal(b1.id, 1);
    assert.equal(b1.tag, "b1");
    assert.equal(b1.name, "Brand1");
    assert.equal(b1.accessToken, "shpat_1");
    assert.equal(b1.shopifyQlTimezone, "Asia/Kolkata");
    assert.equal(b1.pool, pools[0]);
    assert.equal(b1._tablesEnsured, false);
    assert.equal(pools[0].config.password, "dbpw");
    assert.equal(pools[0].config.connectionLimit, 5);
    assert.equal(pools[0].config.ssl, undefined, "non-AWS host => no TLS");

    assert.equal(b2.index, 1);
    assert.equal(b2.tag, "brand_1", "default tag uses running index");
    assert.equal(b2.name, "BRAND_1");
    assert.equal(b2.shopifyQlTimezone, "Asia/Kolkata", "invalid tz falls back");

    for (const call of axios.get.mock.calls) {
      assert.deepEqual(call.arguments[1].headers, { "x-pipeline-key": "secret-header" });
    }
  });

  test("skips inactive, unfetchable, undecryptable and incomplete brands", async () => {
    mockBrandsApi(axios, mysql, {
      1: apiBrand(1, { is_active: false }),
      2: new Error("404"),
      3: apiBrand(3, { access_token: encryptAES("x", "wrong-key") }),
      4: apiBrand(4, { db_host: "" }),
      5: apiBrand(5),
    });
    const brands = await p.loadBrands();
    assert.deepEqual(brands.map((b) => [b.index, b.id]), [[0, 5]]);
  });

  test("auto-enables TLS for *.amazonaws.com hosts", async () => {
    const pools = mockBrandsApi(axios, mysql, {
      1: apiBrand(1, { db_host: "x.rds.amazonaws.com" }),
    });
    await p.loadBrands();
    assert.deepEqual(pools[0].config.ssl, { rejectUnauthorized: false });
  });

  test("DB_SSL_ENABLED=false disables the AWS auto-TLS", async () => {
    process.env.DB_SSL_ENABLED = "false";
    const pools = mockBrandsApi(axios, mysql, { 1: apiBrand(1, { db_host: "x.rds.amazonaws.com" }) });
    await p.loadBrands();
    assert.equal(pools[0].config.ssl, undefined);
  });

  test("DB_SSL_CA enables TLS with the CA (\\n unescaped)", async () => {
    process.env.DB_SSL_CA = "-----BEGIN-----\\nabc\\n-----END-----";
    const pools = mockBrandsApi(axios, mysql, { 1: apiBrand(1) });
    await p.loadBrands();
    assert.deepEqual(pools[0].config.ssl, {
      rejectUnauthorized: false,
      ca: "-----BEGIN-----\nabc\n-----END-----",
    });
  });

  test("exits the process when the brand list cannot be fetched", async () => {
    const exit = trapProcessExit();
    mock.method(axios, "get", async () => {
      throw new Error("down");
    });
    await assert.rejects(p.loadBrands(), /process\.exit\(1\)/);
    assert.equal(exit.mock.callCount(), 1);
  });
});

describe("getBrands", () => {
  test("loads once and caches the result", async () => {
    mockBrandsApi(axios, mysql, { 1: apiBrand(1) });
    const first = await p.getBrands();
    const second = await p.getBrands();
    assert.equal(first, second);
    assert.equal(axios.get.mock.calls.filter((c) => c.arguments[0] === BRANDS_API).length, 1);
  });
});
