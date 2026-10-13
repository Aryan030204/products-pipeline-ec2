// Behaviour when required credentials are absent from the environment.
import { test, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { setTestEnv, silenceConsole, trapProcessExit } from "./helpers.js";

setTestEnv();
for (const key of ["GET_BRANDS_API", "PIPELINE_AUTH_HEADER", "PASSWORD_AES_KEY", "TEST_MODE"]) {
  delete process.env[key];
}
const p = await import("../pipeline.js");

afterEach(() => mock.restoreAll());

test("loadBrands exits when GET_BRANDS_API / PIPELINE_AUTH_HEADER / PASSWORD_AES_KEY are missing", async () => {
  silenceConsole();
  const exit = trapProcessExit();
  await assert.rejects(p.loadBrands(), /process\.exit\(1\)/);
  assert.equal(exit.mock.callCount(), 1);
  assert.match(console.error.mock.calls[0].arguments[0], /Missing required environment variables/);
});

test("decryptAESMaybe requires PASSWORD_AES_KEY for encrypted values", () => {
  assert.equal(p.decryptAESMaybe("plain"), "plain");
  assert.throws(() => p.decryptAESMaybe("iv:cipher"), /PASSWORD_AES_KEY is required/);
});
