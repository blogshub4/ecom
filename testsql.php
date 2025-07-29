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
