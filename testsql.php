-- 2. UPDATE changed rows with changed fields tracking
WITH latest_hist AS (
  SELECT DISTINCT ON (start_ip_int, end_ip_int)
      *
  FROM qu.ip_history_test
  ORDER BY start_ip_int, end_ip_int, lower(systime) DESC
)
INSERT INTO qu.ip_history_test (
   history_id, systime, action,
   start_ip_int, end_ip_int, continent, country, city,
   longt, langt, region, phone, dma, msa, countryiso2,
   changed_fields
)
SELECT
   gen_random_uuid(),
   tstzrange(now_ts, NULL::timestamptz),
   'update',
   CUR.start_ip_int, CUR.end_ip_int, CUR.continent, CUR.country, CUR.city,
   CUR.longt, CUR.langt, CUR.region, CUR.phone, CUR.dma, CUR.msa, CUR.countryiso2,
   ARRAY_REMOVE(ARRAY[
       CASE WHEN CUR.continent   IS DISTINCT FROM LH.continent THEN 'continent' END,
       CASE WHEN CUR.country     IS DISTINCT FROM LH.country THEN 'country' END,
       CASE WHEN CUR.city        IS DISTINCT FROM LH.city THEN 'city' END,
       CASE WHEN CUR.longt       IS DISTINCT FROM LH.longt THEN 'longt' END,
       CASE WHEN CUR.langt       IS DISTINCT FROM LH.langt THEN 'langt' END,
       CASE WHEN CUR.region      IS DISTINCT FROM LH.region THEN 'region' END,
       CASE WHEN CUR.phone       IS DISTINCT FROM LH.phone THEN 'phone' END,
       CASE WHEN CUR.dma         IS DISTINCT FROM LH.dma THEN 'dma' END,
       CASE WHEN CUR.msa         IS DISTINCT FROM LH.msa THEN 'msa' END,
       CASE WHEN CUR.countryiso2 IS DISTINCT FROM LH.countryiso2 THEN 'countryiso2' END
   ], NULL)
FROM qu.ip_test CUR
JOIN latest_hist LH
  ON CUR.start_ip_int = LH.start_ip_int
 AND CUR.end_ip_int   = LH.end_ip_int
WHERE
  (CUR.continent   IS DISTINCT FROM LH.continent) OR
  (CUR.country     IS DISTINCT FROM LH.country)   OR
  (CUR.city        IS DISTINCT FROM LH.city)      OR
  (CUR.longt       IS DISTINCT FROM LH.longt)     OR
  (CUR.langt       IS DISTINCT FROM LH.langt)     OR
  (CUR.region      IS DISTINCT FROM LH.region)    OR
  (CUR.phone       IS DISTINCT FROM LH.phone)     OR
  (CUR.dma         IS DISTINCT FROM LH.dma)       OR
  (CUR.msa         IS DISTINCT FROM LH.msa)       OR
  (CUR.countryiso2 IS DISTINCT FROM LH.countryiso2);



ALTER TABLE qu.ip_history_test
ADD COLUMN changed_fields TEXT[];



CREATE OR REPLACE FUNCTION quova_v7.get_top_changed_rows_with_fields(
    days INT,
    limit_count INT DEFAULT 10
)
RETURNS TABLE (
    country TEXT,
    start_ip_int BIGINT,
    end_ip_int BIGINT,
    change_count BIGINT,
    most_recent_changed_fields TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        country,
        start_ip_int,
        end_ip_int,
        COUNT(*) AS change_count,
        ARRAY(
            SELECT DISTINCT unnest(changed_fields)
            FROM qu.ip_history_test sub
            WHERE sub.start_ip_int = main.start_ip_int
              AND sub.end_ip_int = main.end_ip_int
              AND lower(sub.systime) >= now() - (days || ' days')::interval
        ) AS most_recent_changed_fields
    FROM qu.ip_history_test main
    WHERE lower(systime) >= now() - (days || ' days')::interval
    GROUP BY country, start_ip_int, end_ip_int
    ORDER BY change_count DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;


-- Top 10 changed IPs in last 7 days
SELECT * FROM quova_v7.get_top_changed_rows_with_fields(7, 10);

-- Top 5 changed rows in last 15 days
SELECT * FROM quova_v7.get_top_changed_rows_with_fields(15, 5);




-------------\\\\\\\\\\\\\\\
Update the sync_ip_with_history()

-- 2. UPDATE changed rows
WITH latest_hist AS (
  SELECT DISTINCT ON (start_ip_int, end_ip_int)
      *
  FROM qu.ip_history_test
  ORDER BY start_ip_int, end_ip_int, lower(systime) DESC
)
INSERT INTO qu.ip_history_test (
   history_id, systime, action,
   start_ip_int, end_ip_int, continent, country, city,
   longt, langt, region, phone, dma, msa, countryiso2,
   changed_fields
)
SELECT
   gen_random_uuid(),
   tstzrange(now_ts, NULL::timestamptz),
   'update',
   CUR.start_ip_int, CUR.end_ip_int, CUR.continent, CUR.country, CUR.city,
   CUR.longt, CUR.langt, CUR.region, CUR.phone, CUR.dma, CUR.msa, CUR.countryiso2,
   ARRAY[
     CASE WHEN CUR.continent   IS DISTINCT FROM LH.continent THEN 'continent' ELSE NULL END,
     CASE WHEN CUR.country     IS DISTINCT FROM LH.country THEN 'country' ELSE NULL END,
     CASE WHEN CUR.city        IS DISTINCT FROM LH.city THEN 'city' ELSE NULL END,
     CASE WHEN CUR.longt       IS DISTINCT FROM LH.longt THEN 'longt' ELSE NULL END,
     CASE WHEN CUR.langt       IS DISTINCT FROM LH.langt THEN 'langt' ELSE NULL END,
     CASE WHEN CUR.region      IS DISTINCT FROM LH.region THEN 'region' ELSE NULL END,
     CASE WHEN CUR.phone       IS DISTINCT FROM LH.phone THEN 'phone' ELSE NULL END,
     CASE WHEN CUR.dma         IS DISTINCT FROM LH.dma THEN 'dma' ELSE NULL END,
     CASE WHEN CUR.msa         IS DISTINCT FROM LH.msa THEN 'msa' ELSE NULL END,
     CASE WHEN CUR.countryiso2 IS DISTINCT FROM LH.countryiso2 THEN 'countryiso2' ELSE NULL END
   ]::TEXT[]
FROM qu.ip_test CUR
JOIN latest_hist LH
  ON CUR.start_ip_int = LH.start_ip_int
 AND CUR.end_ip_int   = LH.end_ip_int
WHERE
  (CUR.continent   IS DISTINCT FROM LH.continent) OR
  (CUR.country     IS DISTINCT FROM LH.country)   OR
  (CUR.city        IS DISTINCT FROM LH.city)      OR
  (CUR.longt       IS DISTINCT FROM LH.longt)     OR
  (CUR.langt       IS DISTINCT FROM LH.langt)     OR
  (CUR.region      IS DISTINCT FROM LH.region)    OR
  (CUR.phone       IS DISTINCT FROM LH.phone)     OR
  (CUR.dma         IS DISTINCT FROM LH.dma)       OR
  (CUR.msa         IS DISTINCT FROM LH.msa)       OR
  (CUR.countryiso2 IS DISTINCT FROM LH.countryiso2);


-- Find rows where 'city' or 'phone' changed in last 7 days
SELECT *
FROM qu.ip_history_test
WHERE 'city' = ANY(changed_fields)
  AND lower(systime) >= now() - interval '7 days';



//////////////////////////////////////////////

CREATE OR REPLACE FUNCTION quova_v7.get_advanced_ip_history_stats(
    days INT,
    action_filter record_action DEFAULT NULL,       -- pass NULL for all actions
    country_filter TEXT DEFAULT NULL,               -- pass NULL for all countries
    group_by TEXT DEFAULT 'start_ip_int',           -- 'start_ip_int', 'country', or 'country_ip'
    limit_count INT DEFAULT 10
)
RETURNS TABLE (
    country TEXT,
    start_ip_int BIGINT,
    end_ip_int BIGINT,
    action record_action,
    change_count BIGINT
) AS $$
BEGIN
    RETURN QUERY EXECUTE format($f$
        SELECT
            country,
            start_ip_int,
            end_ip_int,
            action,
            COUNT(*) AS change_count
        FROM qu.ip_history_test
        WHERE lower(systime) >= now() - INTERVAL '%s days'
            %s  -- action filter
            %s  -- country filter
        GROUP BY %s, start_ip_int, end_ip_int, action, country
        ORDER BY change_count DESC
        LIMIT %s
    $f$,
        days,
        CASE 
            WHEN action_filter IS NULL THEN ''
            ELSE format('AND action = %L', action_filter)
        END,
        CASE 
            WHEN country_filter IS NULL THEN ''
            ELSE format('AND country = %L', country_filter)
        END,
        CASE 
            WHEN group_by = 'country' THEN 'country'
            WHEN group_by = 'country_ip' THEN 'country, start_ip_int'
            ELSE 'start_ip_int'
        END,
        limit_count
    );
END;
$$ LANGUAGE plpgsql;

SELECT * FROM quova_v7.get_advanced_ip_history_stats(7);
SELECT * FROM quova_v7.get_advanced_ip_history_stats(
    30, 'update', 'India', 'start_ip_int', 5
);
SELECT * FROM quova_v7.get_advanced_ip_history_stats(
    15, NULL, NULL, 'country', 15
);
SELECT * FROM quova_v7.get_advanced_ip_history_stats(
    15, NULL, NULL, 'country_ip', 10
);






----------------------

CREATE OR REPLACE FUNCTION quova_v7.get_ip_history_by_days(days INT)
RETURNS TABLE (
    changed_at timestamptz,
    action record_action,
    country TEXT,
    start_ip_int BIGINT,
    end_ip_int BIGINT,
    continent TEXT,
    city TEXT,
    longt TEXT,
    langt TEXT,
    region TEXT,
    phone TEXT,
    dma TEXT,
    msa TEXT,
    countryiso2 TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        lower(systime),
        action,
        country,
        start_ip_int,
        end_ip_int,
        continent,
        city,
        longt,
        langt,
        region,
        phone,
        dma,
        msa,
        countryiso2
    FROM qu.ip_history_test
    WHERE lower(systime) >= now() - (days || ' days')::interval;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM quova_v7.get_ip_history_by_days(7);
SELECT * FROM quova_v7.get_ip_history_by_days(15);
SELECT *
FROM quova_v7.get_ip_history_by_days(7)
WHERE country = 'India';
SELECT *
FROM quova_v7.get_ip_history_by_days(15)
WHERE start_ip_int >= 167772160 AND end_ip_int <= 184549375;
CREATE INDEX idx_ip_history_systime ON qu.ip_history_test (lower(systime));
CREATE INDEX idx_ip_history_country ON qu.ip_history_test (country);
CREATE INDEX idx_ip_history_ip_range ON qu.ip_history_test (start_ip_int, end_ip_int);
psql -d your_db -U your_user -c "COPY (SELECT * FROM quova_v7.get_ip_history_by_days(1)) TO '/tmp/ip_report.csv' CSV HEADER;"

Query: Most Frequently Updated start_ip_int in Last N Days
SELECT 
    start_ip_int,
    COUNT(*) AS change_count
FROM qu.ip_history_test
WHERE lower(systime) >= now() - interval '15 days'
GROUP BY start_ip_int
ORDER BY change_count DESC
LIMIT 10;

Query: Most Frequently Updated Countries in Last N Days
SELECT 
    country,
    COUNT(*) AS change_count
FROM qu.ip_history_test
WHERE lower(systime) >= now() - interval '15 days'
GROUP BY country
ORDER BY change_count DESC
LIMIT 10;


Query: Combination of start_ip_int and Country
SELECT 
    country,
    start_ip_int,
    COUNT(*) AS change_count
FROM qu.ip_history_test
WHERE lower(systime) >= now() - interval '15 days'
GROUP BY country, start_ip_int
ORDER BY change_count DESC
LIMIT 10;

Want it Dynamic? Create a Parameterized Function
CREATE OR REPLACE FUNCTION quova_v7.get_top_changes(
    days INT,
    limit_count INT DEFAULT 10
)
RETURNS TABLE (
    country TEXT,
    start_ip_int BIGINT,
    change_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        country,
        start_ip_int,
        COUNT(*) AS change_count
    FROM qu.ip_history_test
    WHERE lower(systime) >= now() - (days || ' days')::interval
    GROUP BY country, start_ip_int
    ORDER BY change_count DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

  use
-- Top 10 in last 7 days
SELECT * FROM quova_v7.get_top_changes(7, 10);

-- Top 5 in last 30 days
SELECT * FROM quova_v7.get_top_changes(30, 5);
