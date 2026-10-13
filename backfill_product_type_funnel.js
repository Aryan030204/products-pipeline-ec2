/**
 * Backfill worker for mv_product_type_funnel_daily ONLY.
 *
 * Re-fetches Shopify's ShopifyQL `sales` report (product_type x day) and
 * re-refreshes the product-type funnel rollup for a date range, without
 * touching the main session snapshot/MVs/hourly tables. Useful for backfilling
 * the new funnel table over history, or re-running it after a bug fix, without
 * re-running the (much heavier) full pipeline backfill.
 *
 * Follows the SAME env configuration pipeline.js's own backfill mode uses, so
 * the same .env / secrets work unchanged:
 *
 * Explicit range:
 *   BACKFILL_START_IST_DATE=2025-10-01
 *   BACKFILL_END_IST_DATE=2025-12-14
 *
 * Rolling window (brand-local last N days; overrides START/END if > 0):
 *   BACKFILL_LOOKBACK_DAYS=2
 *
 * Optional:
 *   BACKFILL_BRAND_INDICES=0,2,5   (limit to specific brands)
 *   BACKFILL_BRAND_CONCURRENCY=2   (default 2, same as pipeline.js)
 *
 * Required (brand loading / Shopify auth — same as pipeline.js):
 *   GET_BRANDS_API, PIPELINE_AUTH_HEADER, PASSWORD_AES_KEY
 *
 * Note: mv_product_type_funnel_daily's `sessions`/`sessions_with_cart_additions`
 * columns are joined from mv_product_sessions_by_type_daily for the same date.
 * If that date was never processed by the main pipeline, those columns will
 * come back as 0 — this script only backfills the sales/orders side and the
 * rollup join, not the underlying session data.
 */

import "dotenv/config";
import { pathToFileURL } from "url";
import {
  getBrands,
  selectBackfillBrands,
  buildInclusiveDateRangeYMD,
  recentDatesForTimeZone,
  mapWithConcurrency,
  ensureTablesForBrand,
} from "./pipeline.js";
import {
  ensureProductTypeSalesTables,
  fetchProductTypeSales,
  upsertProductTypeSalesSnapshot,
  refreshProductTypeFunnelRollup,
} from "./product_type_sales.js";
import { ShopifyQLFetchError } from "./shopifyql_utils.js";

const BACKFILL_START_IST_DATE = (
  process.env.BACKFILL_START_IST_DATE ||
  process.env.BACKFILL_START_IST ||
  ""
).split("T")[0];
const BACKFILL_END_IST_DATE = (
  process.env.BACKFILL_END_IST_DATE ||
  process.env.BACKFILL_END_IST ||
  ""
).split("T")[0];
const BACKFILL_LOOKBACK_DAYS = Math.max(
  0,
  parseInt(process.env.BACKFILL_LOOKBACK_DAYS || "0", 10) || 0,
);
const BACKFILL_BRAND_CONCURRENCY = Math.max(
  1,
  parseInt(process.env.BACKFILL_BRAND_CONCURRENCY || "2", 10) || 2,
);

export async function processBrandDate(brand, targetYmd) {
  await ensureTablesForBrand(brand); // mv_product_sessions_by_type_daily lives here
  await ensureProductTypeSalesTables(brand);

  try {
    const rows = await fetchProductTypeSales(brand, targetYmd);
    await upsertProductTypeSalesSnapshot(brand, rows, targetYmd);
    await refreshProductTypeFunnelRollup(brand, targetYmd);
  } catch (err) {
    if (err instanceof ShopifyQLFetchError) {
      console.error(
        `[${brand.tag}] Skipping product-type funnel backfill for ${targetYmd}: ${err.message}`,
      );
      return;
    }
    throw err;
  }
}

export async function run() {
  const brands = selectBackfillBrands(await getBrands());
  if (!brands.length) {
    console.log(`[FUNNEL-BACKFILL] No active brands to process.`);
    return;
  }

  if (BACKFILL_LOOKBACK_DAYS > 0) {
    console.log(
      `\n🧱 Product-type funnel backfill (rolling: last ${BACKFILL_LOOKBACK_DAYS} brand-local day(s) per brand)\n`,
    );
    await mapWithConcurrency(brands, BACKFILL_BRAND_CONCURRENCY, async (brand) => {
      const dates = recentDatesForTimeZone(brand.shopifyQlTimezone, BACKFILL_LOOKBACK_DAYS);
      console.log(
        `[FUNNEL-BACKFILL] ${brand.tag} (tz=${brand.shopifyQlTimezone}): ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} day(s))`,
      );
      for (const d of dates) {
        await processBrandDate(brand, d);
      }
    });
    console.log(`\n🏁 Product-type funnel backfill complete (rolling).\n`);
    return;
  }

  if (!BACKFILL_START_IST_DATE || !BACKFILL_END_IST_DATE) {
    throw new Error(
      `[FUNNEL-BACKFILL] Requires BACKFILL_START_IST_DATE and BACKFILL_END_IST_DATE (or BACKFILL_LOOKBACK_DAYS for a rolling window)`,
    );
  }

  const dates = buildInclusiveDateRangeYMD(BACKFILL_START_IST_DATE, BACKFILL_END_IST_DATE);
  console.log(
    `\n🧱 Product-type funnel backfill: ${dates[0]} → ${dates[dates.length - 1]} (${dates.length} day(s)) | brand concurrency=${BACKFILL_BRAND_CONCURRENCY}\n`,
  );

  for (const d of dates) {
    console.log(`\n🗓️ [FUNNEL-BACKFILL] Running for ${d} ...\n`);
    await mapWithConcurrency(brands, BACKFILL_BRAND_CONCURRENCY, (brand) =>
      processBrandDate(brand, d),
    );
  }

  console.log(`\n🏁 Product-type funnel backfill complete for ${dates.length} day(s).\n`);
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  run()
    .then(() => {
      console.log(`[FUNNEL-BACKFILL] Done, exiting.`);
      process.exit(0);
    })
    .catch((err) => {
      console.error(`[FUNNEL-BACKFILL] Run failed:`, err);
      process.exit(1);
    });
}
