-- ===========================================================================
-- (e)  ONE QUERY ACROSS BOTH SYSTEMS
--
-- The sale lines live in the object store as Parquet.  The stores, products
-- and categories live in PostgreSQL.  Neither is staged into the other: this
-- reads both where they already are, in a single statement.
--
--   read_parquet('s3://annapurna-lake/...')   -> MinIO, over httpfs
--   pg.stores / pg.products / pg.product_categories
--                                             -> PostgreSQL, over postgres_scanner
--
-- The question: for October 2024, in the South and West regions, how did the
-- Food department sell by region and category -- and what does that come to
-- per square foot of shop floor.  Store attributes (region, floor area) only
-- exist in PostgreSQL; the sale lines only exist in the object store.  There
-- is no way to answer it inside either system alone.
-- ===========================================================================
SELECT
    s.region,
    c.category_name,
    count(DISTINCT s.store_id)                       AS stores,
    count(*)                                         AS sale_lines,
    round(sum(l.qty * l.unit_price), 2)              AS revenue,
    round(sum(l.qty * l.unit_price)
          / max(t.total_sqft), 2)                    AS revenue_per_sqft
FROM read_parquet('s3://annapurna-lake/raw/sales/**/*.parquet', hive_partitioning=1) l
JOIN pg.stores              s ON s.store_id = l.store_id
JOIN pg.products            p ON p.product_code = l.raw_code
                             AND l.business_date BETWEEN p.valid_from AND p.valid_to
JOIN pg.product_categories  c ON c.category_id = p.category_id
JOIN (SELECT region, sum(floor_area_sqft) AS total_sqft
      FROM pg.stores GROUP BY region)  t ON t.region = s.region
WHERE l.business_month = '2024-10'
  AND l.line_type IN ('SALE','RETURN','VOID')
  AND s.region IN ('South','West')
  AND c.department = 'Food'
GROUP BY s.region, c.category_name
ORDER BY s.region, revenue DESC;
