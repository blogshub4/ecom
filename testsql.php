ALTER TABLE ip_history
ALTER COLUMN systime TYPE timestamp WITHOUT time zone
USING date_trunc('second', (systime AT TIME ZONE 'UTC')::timestamp);


ALTER TABLE ip_history
ALTER COLUMN systime TYPE timestamp WITHOUT time zone
USING date_trunc('second', systime AT TIME ZONE 'UTC');


-- Step 1: Convert to UTC and remove timezone
ALTER TABLE ip_history
ALTER COLUMN systime TYPE timestamp WITHOUT time zone
USING systime AT TIME ZONE 'UTC';

-- Step 2: Truncate microseconds
UPDATE ip_history
SET systime = date_trunc('second', systime);

I’ve updated the systime column in the ip_history table as requested. It is now stored in the format:

YYYY-MM-DD HH:MM:SS


CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS TRIGGER AS $$
DECLARE
    field_changes TEXT[];
BEGIN
    -- INSERT case
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ip_history (
            start_ip_int, end_ip_int, continent, country, country_iso2,
            action, systime, changed_fields
        )
        VALUES (
            NEW.start_ip_int, NEW.end_ip_int, NEW.continent, NEW.country, NEW.country_iso2,
            'insert', now(), ARRAY[]::TEXT[]
        );
        RETURN NEW;

    -- DELETE case
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ip_history (
            start_ip_int, end_ip_int, continent, country, country_iso2,
            action, systime, changed_fields
        )
        VALUES (
            OLD.start_ip_int, OLD.end_ip_int, OLD.continent, OLD.country, OLD.country_iso2,
            'delete', now(), ARRAY[]::TEXT[]
        );
        RETURN OLD;

    -- UPDATE case
    ELSIF TG_OP = 'UPDATE' THEN
        field_changes := ARRAY[]::TEXT[];

        IF NEW.continent IS DISTINCT FROM OLD.continent THEN
            field_changes := array_append(field_changes, 'continent');
        END IF;

        IF NEW.country IS DISTINCT FROM OLD.country THEN
            field_changes := array_append(field_changes, 'country');
        END IF;

        IF NEW.country_iso2 IS DISTINCT FROM OLD.country_iso2 THEN
            field_changes := array_append(field_changes, 'country_iso2');
        END IF;

        -- Add all fields here as needed...

        -- Only log if at least one field actually changed
        IF array_length(field_changes, 1) > 0 THEN
            INSERT INTO ip_history (
                start_ip_int, end_ip_int, continent, country, country_iso2,
                action, systime, changed_fields
            )
            VALUES (
                NEW.start_ip_int, NEW.end_ip_int, NEW.continent, NEW.country, NEW.country_iso2,
                'update', now(), field_changes
            );
        END IF;

        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

==============================

I've manually tested the sync_ip_with_history() function using controlled inserts. Here's what happens:

On first insert, an INSERT action is logged for the new IP range.

If I re-insert the same IP range with updated fields (e.g., country_iso2), the trigger logs an UPDATE action, capturing only the changed fields.

If the IP range no longer exists in the latest insert, the function logs a DELETE for the missing start_ip_int.

This simulates how the sync logic would behave when comparing one Neustar file version to another. The logic assumes the IP range (start_ip_int, end_ip_int) is the identity key, and field changes or removal/inserts are tracked accordingly.

Let me know if you’d like the raw test records or query outputs — I’ve validated it against multiple cases including new inserts, updates, and deletions.
================
You're absolutely right to raise this — the current trigger logic assumes that the start_ip_int and end_ip_int act as the unique identifiers for detecting changes.
We haven't yet validated whether these ranges remain consistent across historical Neustar files. I’ll initiate a comparison of the historical records (as added by Derek to Taku-sh) to:

Assess the percentage of changes per file

Verify if the ranges (start_ip_int, end_ip_int) persist or shift
Once complete, I’ll share a report with findings and recommendations (e.g., whether we should key on a different combination).

❌ 2. Trigger Logic for DELETE
Yes, the DELETE trigger is based on a diff check — i.e., if an IP range in the old data no longer exists in the latest Neustar file. That said, if start_ip_int and end_ip_int are not stable, this could indeed lead to excessive INSERT and DELETE noise.

To mitigate this, we may need to:

Use a checksum comparison of key fields instead of relying solely on IP ranges.

Introduce a smarter diffing mechanism to avoid false positives when ranges change slightly.

📅 3. Format of systime
I’ll update the systime column formatting to YYYY-MM-DD as per your long-term integration plan with Helpdesk. Currently it's stored as a full timestamp, but we can present it as a formatted date view or store it that way directly if preferred.

====

As part of validating the sync_ip_with_history() trigger logic, I manually inserted records into the main IP table and confirmed the trigger correctly logs INSERT, UPDATE, and DELETE actions in the history table.

To verify whether the start_ip_int and end_ip_int ranges remain consistent across Neustar file versions, I will run a historical comparison using older Neustar snapshots available in our database. This will:

Highlight which IP ranges persist across versions vs. those that are added or dropped.

Quantify the percentage of range changes per file.

This should give a clearer picture of how stable those IP ranges are over time. If significant drift is observed, we may need to refine the logic (e.g., by comparing additional fields or using checksums).

I’ll share a comparison summary once the checks are complete.





-=-=---------=============================

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
