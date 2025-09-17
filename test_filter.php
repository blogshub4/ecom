CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(p_days integer DEFAULT 7)
RETURNS TABLE (
    total_ip_table1 bigint,
    total_ip_table2 bigint,
    common_ip bigint,
    pct_with_file1 numeric,
    pct_with_file2 numeric,
    pct_union numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH 
    table1 AS (
        SELECT COUNT(DISTINCT (ROW(start_ip_int, end_ip_int))) AS cnt 
        FROM quova_v7.ip_test
    ),
    table2 AS (
        SELECT COUNT(DISTINCT (ROW(start_ip_int, end_ip_int))) AS cnt 
        FROM quova_v7.ip_history_test
        WHERE log_date >= NOW() - (p_days || ' days')::interval
    ),
    common AS (
        SELECT COUNT(DISTINCT (ROW(t1.start_ip_int, t1.end_ip_int))) AS cnt
        FROM quova_v7.ip_test t1
        JOIN quova_v7.ip_history_test t2
          ON t1.start_ip_int = t2.start_ip_int
         AND t1.end_ip_int   = t2.end_ip_int
        WHERE t2.log_date >= NOW() - (p_days || ' days')::interval
    )
    SELECT 
        t1.cnt AS total_ip_table1,
        t2.cnt AS total_ip_table2,
        c.cnt  AS common_ip,
        CASE WHEN t1.cnt > 0 
             THEN ROUND(c.cnt::numeric / t1.cnt * 100, 2) 
             ELSE 0 END AS pct_with_file1,
        CASE WHEN t2.cnt > 0 
             THEN ROUND(c.cnt::numeric / t2.cnt * 100, 2) 
             ELSE 0 END AS pct_with_file2,
        CASE WHEN (t1.cnt + t2.cnt - c.cnt) > 0
             THEN ROUND(c.cnt::numeric / (t1.cnt + t2.cnt - c.cnt) * 100, 2)
             ELSE 0 END AS pct_union
    FROM table1 t1, table2 t2, common c;
END;
$$;



CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(p_days integer DEFAULT 7)
RETURNS TABLE (
    total_ip_table1 bigint,
    total_ip_table2 bigint,
    common_ip bigint,
    pct_with_file1 numeric,
    pct_with_file2 numeric,
    pct_union numeric
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH 
    table1 AS (
        SELECT COUNT(*) AS cnt 
        FROM quova_v7.ip_test
    ),
    table2 AS (
        SELECT COUNT(*) AS cnt 
        FROM quova_v7.ip_history_test
        WHERE log_date >= NOW() - (p_days || ' days')::interval
    ),
    common AS (
        SELECT COUNT(*) AS cnt
        FROM quova_v7.ip_test t1
        JOIN quova_v7.ip_history_test t2
          ON t1.start_ip_int = t2.start_ip_int
         AND t1.end_ip_int   = t2.end_ip_int
        WHERE t2.log_date >= NOW() - (p_days || ' days')::interval
    )
    SELECT 
        t1.cnt AS total_ip_table1,
        t2.cnt AS total_ip_table2,
        c.cnt  AS common_ip,
        CASE WHEN t1.cnt > 0 
             THEN ROUND(c.cnt::numeric / t1.cnt * 100, 2) 
             ELSE 0 END AS pct_with_file1,
        CASE WHEN t2.cnt > 0 
             THEN ROUND(c.cnt::numeric / t2.cnt * 100, 2) 
             ELSE 0 END AS pct_with_file2,
        CASE WHEN (t1.cnt + t2.cnt - c.cnt) > 0
             THEN ROUND(c.cnt::numeric / (t1.cnt + t2.cnt - c.cnt) * 100, 2)
             ELSE 0 END AS pct_union
    FROM table1 t1, table2 t2, common c;
END;
$$;




CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(dummy integer DEFAULT 0)
RETURNS TABLE (
    total_ip_table1 bigint,
    total_ip_table2 bigint,
    common_ip bigint,
    ip_pct_file1 numeric,
    ip_pct_file2 numeric,
    ip_pct_union numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH 
    table1 AS (
        SELECT COUNT(*) AS cnt FROM ip_test
    ),
    table2 AS (
        SELECT COUNT(*) AS cnt FROM ip_history_test
    ),
    common AS (
        SELECT COUNT(*) AS cnt
        FROM ip_test t1
        JOIN ip_history_test t2
          ON t1.start_ip_int = t2.start_ip_int
         AND t1.end_ip_int = t2.end_ip_int
         -- include more fields if you want stricter comparison
    )
    SELECT 
        t1.cnt AS total_ip_table1,
        t2.cnt AS total_ip_table2,
        c.cnt AS common_ip,
        CASE WHEN t1.cnt > 0 THEN ROUND(c.cnt::numeric / t1.cnt * 100, 2) ELSE 0 END AS ip_pct_file1,
        CASE WHEN t2.cnt > 0 THEN ROUND(c.cnt::numeric / t2.cnt * 100, 2) ELSE 0 END AS ip_pct_file2,
        CASE 
            WHEN (t1.cnt + t2.cnt - c.cnt) > 0 
            THEN ROUND(c.cnt::numeric / (t1.cnt + t2.cnt - c.cnt) * 100, 2) 
            ELSE 0 
        END AS ip_pct_union
    FROM table1 t1, table2 t2, common c;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(p_days INTEGER DEFAULT 7)
RETURNS TABLE (
    ip_only_count INTEGER,
    history_only_count INTEGER,
    common_count INTEGER,
    total INTEGER,
    ip_only_pct NUMERIC,
    history_only_pct NUMERIC,
    common_pct NUMERIC
)
LANGUAGE plpgsql AS $$
DECLARE
    v_ip_cnt INTEGER;
    v_history_cnt INTEGER;
    v_common_cnt INTEGER;
BEGIN
    -- Get counts into variables first
    SELECT COUNT(DISTINCT ROW(start_ip_int, end_ip_int))
    INTO v_ip_cnt
    FROM quova_v7.ip_test;

    SELECT COUNT(DISTINCT ROW(start_ip_int, end_ip_int))
    INTO v_history_cnt
    FROM quova_v7.ip_history_test
    WHERE log_date >= CURRENT_DATE - p_days;

    SELECT COUNT(DISTINCT ROW(i.start_ip_int, i.end_ip_int))
    INTO v_common_cnt
    FROM quova_v7.ip_test i
    JOIN quova_v7.ip_history_test h
      ON i.start_ip_int = h.start_ip_int
     AND i.end_ip_int   = h.end_ip_int
    WHERE h.log_date >= CURRENT_DATE - p_days;

    -- Debug output
    RAISE NOTICE 'IP count = %, History count = %, Common count = %',
                 v_ip_cnt, v_history_cnt, v_common_cnt;

    -- Return the final results
    RETURN QUERY
    SELECT
        (v_ip_cnt - v_common_cnt) AS ip_only_count,
        (v_history_cnt - v_common_cnt) AS history_only_count,
        v_common_cnt AS common_count,
        (v_ip_cnt + v_history_cnt - v_common_cnt) AS total,
        ROUND(((v_ip_cnt - v_common_cnt)::NUMERIC / NULLIF((v_ip_cnt + v_history_cnt - v_common_cnt),0)) * 100, 2) AS ip_only_pct,
        ROUND(((v_history_cnt - v_common_cnt)::NUMERIC / NULLIF((v_ip_cnt + v_history_cnt - v_common_cnt),0)) * 100, 2) AS history_only_pct,
        ROUND((v_common_cnt::NUMERIC / NULLIF((v_ip_cnt + v_history_cnt - v_common_cnt),0)) * 100, 2) AS common_pct;
END;
$$;



==========================

CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(p_days INT DEFAULT 7)
RETURNS TABLE (
    ip_only_count BIGINT,
    history_only_count BIGINT,
    common_count BIGINT,
    total BIGINT,
    ip_only_pct NUMERIC,
    history_only_pct NUMERIC,
    common_pct NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_ip BIGINT := 0;
    v_history BIGINT := 0;
    v_common BIGINT := 0;
    v_total BIGINT := 0;
BEGIN
    -- total records in ip_test (not time-based since no log_date there)
    SELECT COUNT(*) INTO v_ip
    FROM quova_v7.ip_test;

    -- total records in ip_history_test within time window
    SELECT COUNT(*) INTO v_history
    FROM quova_v7.ip_history_test h
    WHERE h.log_date >= NOW() - (p_days || ' days')::INTERVAL;

    -- common (records in both tables, active history only, within time window)
    SELECT COUNT(*) INTO v_common
    FROM quova_v7.ip_test t
    JOIN quova_v7.ip_history_test h
      ON t.start_ip_int = h.start_ip_int
     AND h.active = TRUE
    WHERE h.log_date >= NOW() - (p_days || ' days')::INTERVAL;

    -- total = union (ip + history - common)
    v_total := v_ip + v_history - v_common;

    -- ip_only = ip - common
    ip_only_count := v_ip - v_common;

    -- history_only = history - common
    history_only_count := v_history - v_common;

    common_count := v_common;
    total        := v_total;

    -- percentages
    ip_only_pct       := CASE WHEN v_total > 0 THEN ROUND(100.0 * (v_ip - v_common) / v_total, 2) ELSE 0 END;
    history_only_pct  := CASE WHEN v_total > 0 THEN ROUND(100.0 * (v_history - v_common) / v_total, 2) ELSE 0 END;
    common_pct        := CASE WHEN v_total > 0 THEN ROUND(100.0 * v_common / v_total, 2) ELSE 0 END;

    RETURN NEXT;
END;
$$;


--------------

CREATE OR REPLACE FUNCTION quova_v7.get_ip_sync_stats(p_days INT DEFAULT 7)
RETURNS TABLE (
    ip_only_count BIGINT,
    history_only_count BIGINT,
    common_count BIGINT,
    total BIGINT,
    ip_only_pct NUMERIC,
    history_only_pct NUMERIC,
    common_pct NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_ip_only BIGINT := 0;
    v_history_only BIGINT := 0;
    v_common BIGINT := 0;
    v_total BIGINT := 0;
BEGIN
    -- ip_only (records present in ip_test but not in active history)
    SELECT COUNT(*) INTO v_ip_only
    FROM quova_v7.ip_test t
    LEFT JOIN quova_v7.ip_history_test h
      ON t.start_ip_int = h.start_ip_int
     AND h.active = TRUE
    WHERE h.start_ip_int IS NULL;

    -- history_only (records in history but not in ip_test, within time window)
    SELECT COUNT(*) INTO v_history_only
    FROM quova_v7.ip_history_test h
    LEFT JOIN quova_v7.ip_test t
      ON h.start_ip_int = t.start_ip_int
    WHERE t.start_ip_int IS NULL
      AND h.log_date >= NOW() - (p_days || ' days')::INTERVAL;

    -- common (records in both ip_test and active history, within time window)
    SELECT COUNT(*) INTO v_common
    FROM quova_v7.ip_test t
    JOIN quova_v7.ip_history_test h
      ON t.start_ip_int = h.start_ip_int
     AND h.active = TRUE
    WHERE h.log_date >= NOW() - (p_days || ' days')::INTERVAL;

    -- totals
    v_total := v_ip_only + v_history_only + v_common;

    -- assign output columns
    ip_only_count      := v_ip_only;
    history_only_count := v_history_only;
    common_count       := v_common;
    total              := v_total;

    ip_only_pct       := CASE WHEN v_total > 0 THEN ROUND(100.0 * v_ip_only / v_total, 2) ELSE 0 END;
    history_only_pct  := CASE WHEN v_total > 0 THEN ROUND(100.0 * v_history_only / v_total, 2) ELSE 0 END;
    common_pct        := CASE WHEN v_total > 0 THEN ROUND(100.0 * v_common / v_total, 2) ELSE 0 END;

    RETURN NEXT;
END;
$$;







///////////////////////

CREATE OR REPLACE FUNCTION quova_v7.get_top_changed_rows_with_fields(
    p_days INTEGER DEFAULT 7,
    p_limit INTEGER DEFAULT 10
)
RETURNS TABLE (
    start_ip_int BIGINT,
    end_ip_int BIGINT,
    changed_fields TEXT[],
    change_count INTEGER,
    country TEXT,
    city TEXT,
    log_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    active BOOLEAN,
    change_type TEXT, -- New: 'insert', 'update', 'delete'
    change_percentage NUMERIC -- New: percentage of this type
)
AS $$
DECLARE
    total_records INTEGER;
    updated_count INTEGER;
    inserted_count INTEGER;
    deleted_count INTEGER;
BEGIN
    -- Total records within time window
    SELECT COUNT(*) INTO total_records
    FROM quova_v7.ip_history_test
    WHERE log_date >= NOW() - INTERVAL '1 day' * p_days;

    -- Count of updated records
    SELECT COUNT(*) INTO updated_count
    FROM quova_v7.ip_history_test
    WHERE log_date >= NOW() - INTERVAL '1 day' * p_days
      AND changed_fields IS NOT NULL
      AND cardinality(changed_fields) > 0;

    -- Count of inserted records: assuming inserted means first time seen
    SELECT COUNT(*) INTO inserted_count
    FROM (
        SELECT start_ip_int, end_ip_int, MIN(log_date) AS first_seen
        FROM quova_v7.ip_history_test
        GROUP BY start_ip_int, end_ip_int
        HAVING MIN(log_date) >= NOW() - INTERVAL '1 day' * p_days
    ) AS inserts;

    -- Count of deleted records: previously active, now inactive
    SELECT COUNT(*) INTO deleted_count
    FROM (
        SELECT DISTINCT ON (start_ip_int, end_ip_int)
            start_ip_int, end_ip_int, active
        FROM quova_v7.ip_history_test
        WHERE log_date >= NOW() - INTERVAL '1 day' * p_days
        ORDER BY start_ip_int, end_ip_int, log_date DESC
    ) AS latest
    WHERE active = FALSE;

    -- Main data query
    RETURN QUERY
    WITH history_window AS (
        SELECT *
        FROM quova_v7.ip_history_test h
        WHERE h.log_date >= NOW() - INTERVAL '1 day' * p_days
          AND h.changed_fields IS NOT NULL
          AND cardinality(h.changed_fields) > 0
    ),
    exploded_fields AS (
        SELECT 
            h.start_ip_int,
            h.end_ip_int,
            unnest(h.changed_fields) AS field
        FROM history_window h
    ),
    aggregated_changes AS (
        SELECT 
            ef.start_ip_int,
            ef.end_ip_int,
            array_agg(DISTINCT ef.field) AS all_changed_fields,
            COUNT(*) AS total_changes
        FROM exploded_fields ef
        GROUP BY ef.start_ip_int, ef.end_ip_int
    ),
    latest_active AS (
        SELECT DISTINCT ON (h.start_ip_int, h.end_ip_int)
            h.start_ip_int,
            h.end_ip_int,
            ac.all_changed_fields AS changed_fields,
            ac.total_changes::INTEGER AS change_count,
            h.country::TEXT,
            h.city::TEXT,
            h.log_date,
            h.end_date,
            h.active
        FROM quova_v7.ip_history_test h
        JOIN aggregated_changes ac
          ON h.start_ip_int = ac.start_ip_int
         AND h.end_ip_int = ac.end_ip_int
        WHERE h.active = TRUE
        ORDER BY h.start_ip_int, h.end_ip_int, h.log_date DESC
    )
    SELECT 
        *,
        'update'::TEXT AS change_type,
        ROUND(updated_count::NUMERIC / NULLIF(total_records, 0) * 100, 2) AS change_percentage
    FROM latest_active
    ORDER BY change_count DESC, log_date DESC
    LIMIT p_limit;

    -- Optionally: Add UNION ALL to add one row for insert and one for delete statistics
END;
$$ LANGUAGE plpgsql STABLE;




=====================


CREATE OR REPLACE FUNCTION quova_v7.get_change_summary(
    p_days INTEGER DEFAULT 7
)
RETURNS TABLE (
    change_type TEXT,
    record_count INTEGER,
    percentage NUMERIC
)
AS $$
DECLARE
    total_records INTEGER;
BEGIN
    -- Count all records in the time window
    SELECT COUNT(*) INTO total_records
    FROM quova_v7.ip_history_test
    WHERE log_date >= NOW() - INTERVAL '1 day' * p_days;

    -- Return the breakdown of changes
    RETURN QUERY

    -- Updated records
    SELECT 'update'::TEXT, COUNT(*)::INTEGER,
           ROUND(COUNT(*)::NUMERIC / NULLIF(total_records, 0) * 100, 2)
    FROM quova_v7.ip_history_test
    WHERE log_date >= NOW() - INTERVAL '1 day' * p_days
      AND changed_fields IS NOT NULL
      AND cardinality(changed_fields) > 0

    UNION ALL

    -- Inserted records (first time seen within the time range)
    SELECT 'insert'::TEXT, COUNT(*)::INTEGER,
           ROUND(COUNT(*)::NUMERIC / NULLIF(total_records, 0) * 100, 2)
    FROM (
        SELECT start_ip_int, end_ip_int, MIN(log_date) AS first_seen
        FROM quova_v7.ip_history_test
        GROUP BY start_ip_int, end_ip_int
        HAVING MIN(log_date) >= NOW() - INTERVAL '1 day' * p_days
    ) inserts

    UNION ALL

    -- Deleted records (latest version marked as inactive)
    SELECT 'delete'::TEXT, COUNT(*)::INTEGER,
           ROUND(COUNT(*)::NUMERIC / NULLIF(total_records, 0) * 100, 2)
    FROM (
        SELECT DISTINCT ON (start_ip_int, end_ip_int)
            start_ip_int, end_ip_int, active
        FROM quova_v7.ip_history_test
        WHERE log_date >= NOW() - INTERVAL '1 day' * p_days
        ORDER BY start_ip_int, end_ip_int, log_date DESC
    ) latest
    WHERE active = FALSE;
END;
$$ LANGUAGE plpgsql STABLE;
