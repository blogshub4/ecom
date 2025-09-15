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
