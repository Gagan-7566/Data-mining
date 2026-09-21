-- ===========================================================================
-- (d)  THE PERIOD REPORT
--
-- One query.  One parameter: $period, e.g. '2024-03' or '2024-11'.
--
-- Everything that has to move when the reporting period moves -- which rows
-- are in scope, which product a code meant, and which price was in force --
-- is derived from the period, not written into the SQL.  Ask it for March and
-- it answers with March's products at March's prices.  Ask it for last month
-- and it answers with last month's.  The text below does not change between
-- the two.
--
-- Three separate as-of joins are doing that work:
--
--   1. scope        f.business_date BETWEEN period_start AND period_end
--   2. which product   products.valid_from / valid_to   (codes were reissued)
--   3. which price     price_revisions.effective_from / effective_to
--
-- The join to price_revisions is on the DAY OF THE SALE, not on the period
-- end and not on today.  A price that changed mid-month is therefore honoured
-- mid-month, which is what "March is March" has to mean if a price moved on
-- the 14th.
-- ===========================================================================
WITH period AS (
    SELECT CAST($period || '-01' AS DATE)              AS period_start,
           last_day(CAST($period || '-01' AS DATE))    AS period_end
),
lines AS (
    SELECT f.*
    FROM   fact_sale_line f, period p
    WHERE  f.business_date BETWEEN p.period_start AND p.period_end
),
priced AS (
    SELECT
        l.store_id,
        l.business_date,
        l.line_type,
        l.qty,
        l.unit_price                       AS price_printed_on_the_bill,
        pr.selling_price                   AS price_in_force_that_day,
        pr.mrp                             AS mrp_in_force_that_day,
        dp.product_sk,
        dp.product_code,
        dp.product_name,
        dp.category_id,
        dp.category_name,
        l.line_amount                      AS revenue_as_billed,
        CAST(l.qty * coalesce(pr.selling_price, l.unit_price) AS DECIMAL(18,4))
                                           AS revenue_at_price_in_force
    FROM lines l
    JOIN dim_product dp
      ON dp.product_sk = l.product_sk
    -- as-of price: the one revision whose window contains the day of the sale
    LEFT JOIN price_revisions pr
      ON  pr.product_sk = l.product_sk
      AND l.business_date BETWEEN pr.effective_from AND pr.effective_to
)
SELECT
    $period                                             AS reporting_period,
    category_id,
    category_name,
    count(*)                                            AS lines,
    round(sum(revenue_as_billed), 2)                    AS revenue_as_billed,
    round(sum(revenue_at_price_in_force), 2)            AS revenue_at_price_in_force,
    round(sum(revenue_at_price_in_force)
        - sum(revenue_as_billed), 2)                    AS till_sync_drift
FROM priced
GROUP BY ALL
ORDER BY revenue_as_billed DESC;
