DROP TABLE quova_v7.ip_history_test_18_8_2025;
ALTER TABLE quova_v7.ip_history_test DETACH PARTITION quova_v7.ip_history_test_18_8_2025;


CREATE TABLE IF NOT EXISTS quova_v7.ip_history_test (
    history_id UUID NOT NULL,
    start_ip_int BIGINT NOT NULL,
    end_ip_int BIGINT NOT NULL,
    country VARCHAR(20),
    country_iso2 VARCHAR(2),
    city VARCHAR(50),
    changed_fields TEXT[],
    log_date DATE NOT NULL,
    end_date DATE,
    active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (history_id, log_date)
) PARTITION BY RANGE (log_date);

Function to Create Weekly Partitions

CREATE OR REPLACE FUNCTION quova_v7.create_weekly_partitions(p_weeks_ahead INT)
RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    start_date DATE;
    end_date   DATE;
    week       INT := 0;
    partition_name TEXT;
BEGIN
    -- Find the Monday of current week
    start_date := date_trunc('week', current_date)::date;
    
    WHILE week < p_weeks_ahead LOOP
        end_date := start_date + interval '7 days';

        partition_name := format(
            'ip_history_test_%s',
            to_char(start_date, 'IYYY"w"IW')
        );

        -- Create partition if it does not exist
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS quova_v7.%I
             PARTITION OF quova_v7.ip_history_test
             FOR VALUES FROM (%L) TO (%L);',
            partition_name, start_date, end_date
        );

        -- Move to next week
        start_date := end_date;
        week := week + 1;
    END LOOP;
END;
$$;


Run It To create partitions for the next 12 weeks:

SELECT quova_v7.create_weekly_partitions(12);

        This will create partitions like:

ip_history_test_2025w34 → 2025-08-18 to 2025-08-25

ip_history_test_2025w35 → 2025-08-25 to 2025-09-01
        
----------------

CREATE OR REPLACE FUNCTION quova_v7.create_weekly_partition(start_date DATE) 
RETURNS void AS $$
DECLARE
    end_date DATE := start_date + INTERVAL '7 days';
    partition_name TEXT := 'ip_history_test_' || to_char(start_date, 'IYYY"W"IW');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS quova_v7.%I PARTITION OF quova_v7.ip_history_test
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );
END;
$$ LANGUAGE plpgsql;

call
SELECT quova_v7.create_weekly_partition('2025-08-18');
SELECT quova_v7.create_weekly_partition('2025-08-25');



==========================
-- 1️⃣ Make sure pg_partman extension is available
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- 2️⃣ Backup the existing table
CREATE TABLE IF NOT EXISTS quova_v7.ip_history_test_backup AS
SELECT * FROM quova_v7.ip_history_test;

-- 3️⃣ Rename old table
ALTER TABLE quova_v7.ip_history_test RENAME TO ip_history_test_old;

-- 4️⃣ Create new partitioned parent table
CREATE TABLE quova_v7.ip_history_test (
    history_id BIGSERIAL PRIMARY KEY,
    start_ip_int BIGINT,
    end_ip_int BIGINT,
    country TEXT,
    city TEXT,
    changed_fields TEXT[],
    log_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ,
    active BOOLEAN
) PARTITION BY RANGE (log_date);

-- 5️⃣ Configure pg_partman for weekly partitions
SELECT partman.create_parent(
    p_parent_table     := 'quova_v7.ip_history_test',
    p_control          := 'log_date',
    p_type             := 'native',
    p_interval         := '7 days',
    p_premake          := 4,  -- Create partitions 4 weeks ahead
    p_start_partition  := date_trunc('week', NOW()) - interval '12 weeks'
);

-- 6️⃣ Migrate existing rows into new partitioned table
INSERT INTO quova_v7.ip_history_test
SELECT * FROM quova_v7.ip_history_test_old;

-- 7️⃣ Optional: Drop old table after verifying migration
-- DROP TABLE quova_v7.ip_history_test_old;

-- 8️⃣ (Run Daily) Maintain partitions automatically
-- You must set this in cron or pgAgent:
-- SELECT partman.run_maintenance();





/var/www/html/scripts/create_keytab.sh

#!/bin/bash

PRINCIPAL=$1
PASSWORD=$2
KEYTAB_PATH=$3

expect <<EOF
spawn ktutil
expect "ktutil:"
send "addent -password -p $PRINCIPAL -k 1 -e aes256-cts-hmac-sha1-96\r"
expect "Password for $PRINCIPAL:"
send "$PASSWORD\r"
expect "ktutil:"
send "wkt $KEYTAB_PATH\r"
expect "ktutil:"
send "quit\r"
EOF

  init_keytab.php
  <?php
$principal = 'appuser@INT.BAR.COM';      // Update with your real principal
$password = 'YourStrongPassword123';     // Preferably load this from a secret vault or env var
$keytabPath = '/tmp/appuser.keytab';     // Path where keytab will be stored

$scriptPath = '/var/www/html/scripts/create_keytab.sh';
$cmd = escapeshellcmd("$scriptPath '$principal' '$password' '$keytabPath'");

// Optional: log output
exec($cmd . ' 2>&1', $output, $status);

if ($status === 0) {
    echo "✅ Keytab created successfully.<br>";
} else {
    echo "❌ Keytab creation failed with status $status.<br>";
    echo nl2br(htmlspecialchars(implode("\n", $output)));
}
?>

