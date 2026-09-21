-- ===========================================================================
-- (d)  "What did this pack sell for in <period>?"
--
-- The category manager's question, and the one the spreadsheet answered with
-- today's shelf price.  Same two parameters every time: $period and $code.
-- The answer is taken from price_revisions as of that period, and the product
-- the code referred to is resolved as of that period too -- because two dozen
-- codes were reissued in June 2024 and the code alone no longer says what was
-- in the bag.
-- ===========================================================================
WITH period AS (
    SELECT CAST($period || '-01' AS DATE)           AS period_start,
           last_day(CAST($period || '-01' AS DATE)) AS period_end
)
SELECT
    $period                                  AS asked_about,
    dp.product_code,
    dp.product_sk,
    dp.product_name                          AS what_that_code_meant_then,
    dp.category_name                         AS category_then,
    pr.selling_price                         AS selling_price_then,
    pr.mrp                                   AS mrp_then,
    pr.effective_from,
    pr.effective_to
FROM dim_product dp
CROSS JOIN period p
JOIN price_revisions pr
  ON  pr.product_sk = dp.product_sk
  -- the revision in force at the end of the period being reported
  AND p.period_end BETWEEN pr.effective_from AND pr.effective_to
WHERE dp.product_code = $code
  AND p.period_end BETWEEN dp.valid_from AND dp.valid_to
ORDER BY dp.product_sk;
