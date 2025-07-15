Create ip_snapshot table
CREATE TABLE IF NOT EXISTS ip_snapshot AS TABLE ip WITH NO DATA;
Create the sync_ip_with_history() function
CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS VOID AS $$
DECLARE
    current_time TIMESTAMPTZ := now();
BEGIN
    -- INSERTED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'inserted',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
    FROM ip
    LEFT JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE snap.start_ipint IS NULL;

    -- UPDATED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'updated',
        ip.start_ipint, ip.end_ip_int, ip.continent, ip.country, ip.city,
        ip.longt, ip.langt, ip.region, ip.phone, ip.dma, ip.msa
    FROM ip
    JOIN ip_snapshot snap
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE ip.continent IS DISTINCT FROM snap.continent
       OR ip.country IS DISTINCT FROM snap.country
       OR ip.city IS DISTINCT FROM snap.city
       OR ip.longt IS DISTINCT FROM snap.longt
       OR ip.langt IS DISTINCT FROM snap.langt
       OR ip.region IS DISTINCT FROM snap.region
       OR ip.phone IS DISTINCT FROM snap.phone
       OR ip.dma IS DISTINCT FROM snap.dma
       OR ip.msa IS DISTINCT FROM snap.msa;

    -- DELETED records
    INSERT INTO ip_history (
        system, action,
        start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa
    )
    SELECT
        tstzrange(current_time, NULL), 'deleted',
        snap.start_ipint, snap.end_ip_int, snap.continent, snap.country, snap.city,
        snap.longt, snap.langt, snap.region, snap.phone, snap.dma, snap.msa
    FROM ip_snapshot snap
    LEFT JOIN ip
      ON ip.start_ipint = snap.start_ipint AND ip.end_ip_int = snap.end_ip_int
    WHERE ip.start_ipint IS NULL;

    -- Refresh snapshot
    TRUNCATE ip_snapshot;
    INSERT INTO ip_snapshot SELECT * FROM ip;

END;
$$ LANGUAGE plpgsql;

Create the trigger function to call sync
CREATE OR REPLACE FUNCTION trigger_sync_ip_history()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM sync_ip_with_history();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

Attach the trigger to ip
DROP TRIGGER IF EXISTS sync_ip_on_insert ON ip;

CREATE TRIGGER sync_ip_on_insert
AFTER INSERT ON ip
FOR EACH STATEMENT
EXECUTE FUNCTION trigger_sync_ip_history();





==============================================

CREATE OR REPLACE FUNCTION sync_ip_with_history()
RETURNS void AS $$
DECLARE
    current_time TIMESTAMPTZ := now();
BEGIN
    -- 1. Create a temporary backup of the current IP data
    DROP TABLE IF EXISTS ip_backup;
    CREATE TEMP TABLE ip_backup AS TABLE ip;

    -- 2. Truncate and reload the ip table (assumed handled outside this function)

    -- 3. Insert new records
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        i.start_ipint, i.end_ip_int, i.continent, i.country, i.city, i.longt, i.langt, i.region, i.phone, i.dma, i.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'inserted'
    FROM ip i
    LEFT JOIN ip_backup b ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE b.start_ipint IS NULL;

    -- 4. Insert updated records (compare all columns)
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        i.start_ipint, i.end_ip_int, i.continent, i.country, i.city, i.longt, i.langt, i.region, i.phone, i.dma, i.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'updated'
    FROM ip i
    JOIN ip_backup b ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE (
        i.continent IS DISTINCT FROM b.continent OR
        i.country IS DISTINCT FROM b.country OR
        i.city IS DISTINCT FROM b.city OR
        i.longt IS DISTINCT FROM b.longt OR
        i.langt IS DISTINCT FROM b.langt OR
        i.region IS DISTINCT FROM b.region OR
        i.phone IS DISTINCT FROM b.phone OR
        i.dma IS DISTINCT FROM b.dma OR
        i.msa IS DISTINCT FROM b.msa
        -- Add IS DISTINCT FROM for all other 30 columns
    );

    -- 5. Insert deleted records
    INSERT INTO ip_history (
        history_id, start_ipint, end_ip_int, continent, country, city, longt, langt, region, phone, dma, msa,
        -- all other fields...
        system, action
    )
    SELECT
        gen_random_uuid(),
        b.start_ipint, b.end_ip_int, b.continent, b.country, b.city, b.longt, b.langt, b.region, b.phone, b.dma, b.msa,
        -- other fields...
        tstzrange(current_time, NULL), 'deleted'
    FROM ip_backup b
    LEFT JOIN ip i ON i.start_ipint = b.start_ipint AND i.end_ip_int = b.end_ip_int
    WHERE i.start_ipint IS NULL;
END;
$$ LANGUAGE plpgsql;









<?php
$uid = posix_getuid();
putenv("KRB5CCNAME=FILE:/tmp/krb5cc_$uid");

$keytab = '/path/to/your.keytab';
$principal = 'your_user@YOUR.REALM.COM';

// Run kinit
exec("kinit -k -t $keytab $principal 2>&1", $output, $status);

if ($status !== 0) {
    echo "kinit failed:\n";
    echo implode("\n", $output);
    exit;
}

echo "kinit successful\n";

// List the ticket
exec("klist", $klistOutput);
echo implode("\n", $klistOutput);



======================================
1
<?php
$principal = "youruser@YOUR.REALM.COM"; // Replace with your principal
$keytab = "/path/to/your.keytab";       // Replace with the actual keytab path

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Run kinit using the keytab
exec("kinit -k -t $keytab $principal", $output, $return_var);

if ($return_var !== 0) {
    echo "Kerberos ticket initialization failed.\n";
    print_r($output);
    exit;
}
?>
<?php
$dbHost = 'your-db-host.example.com'; // DNS name, not IP
$dbPort = '5432';
$dbName = 'your_database';
$krbUser = 'appuser'; // This maps from Kerberos to DB role via pg_ident.conf

$dsn = "pgsql:host=$dbHost;port=$dbPort;dbname=$dbName";

try {
    $pdo = new PDO($dsn, $krbUser, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
    echo "✅ Kerberos-authenticated DB connection successful!";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage();
}
?>


2====================

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Run kinit using the keytab
$principal = 'youruser@INT.BAR.COM';  // Replace with your principal
$keytab = '/path/to/your.keytab';     // Make sure this is readable

exec("kinit -k -t $keytab $principal", $output, $status);

if ($status !== 0) {
    die("❌ Kerberos kinit failed: " . implode("\n", $output));
}


<?php
ini_set('display_errors', 1);
error_reporting(E_ALL);

putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . posix_getuid());

// Step 1: Authenticate via keytab
$principal = 'youruser@INT.BAR.COM';         // Your Kerberos principal
$keytab = '/path/to/your.keytab';            // Your keytab file
exec("kinit -k -t $keytab $principal", $output, $status);

if ($status !== 0) {
    die("Kerberos auth failed:\n" . implode("\n", $output));
}

// Step 2: Connect to PostgreSQL using Kerberos (GSSAPI)
$dsn = "pgsql:host=dbserver.int.bar.com;port=5432;dbname=yourdb";
$user = $principal; // or a mapped DB role like 'appuser'
$password = null;

try {
    $pdo = new PDO($dsn, $user, $password, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
    echo "✅ Connected to database successfully using Kerberos.";
} catch (PDOException $e) {
    die("❌ Connection failed: " . $e->getMessage());
}
?>



==============

<?php

// Configuration (⚠️ DO NOT hardcode password in production)
$principal = "youruser@INT.BAR.COM";
$password  = "yourpassword"; // Insecure! Use only in test env

// Create temp file for password input
$tmpPasswordFile = tempnam("/tmp", "krb_pass");
file_put_contents($tmpPasswordFile, $password . "\n");

// Run kinit with the password file
$cmd = "kinit {$principal} < {$tmpPasswordFile}";
exec($cmd, $output, $returnVar);

// Cleanup
unlink($tmpPasswordFile);

if ($returnVar !== 0) {
    echo "❌ kinit failed.\n";
    exit;
}

// Set Kerberos ticket cache environment
$uid = function_exists('posix_getuid') ? posix_getuid() : getmyuid();
putenv("KRB5CCNAME=FILE:/tmp/krb5cc_" . $uid);

// Now try connecting to PostgreSQL using Kerberos
$dsn = "pgsql:host=grdsrv001234.INT.BAR.COM;port=5432;dbname=your_db";
$username = $principal;  // youruser@INT.BAR.COM
$password = ""; // No password needed, Kerberos ticket used

try {
    $pdo = new PDO($dsn, $username, $password);
    echo "✅ Connected to PostgreSQL using Kerberos.\n";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage() . "\n";
}



<?php
putenv("KRB5CCNAME=/tmp/krb5cc_" . posix_getuid());

$output = [];
exec("klist", $output);

$principal = null;

foreach ($output as $line) {
    if (stripos($line, 'Default principal:') !== false) {
        $parts = explode(':', $line);
        $principal = trim($parts[1]);
        break;
    }
}

if ($principal) {
    echo "Kerberos principal is: $principal\n";
} else {
    echo "Principal not found or ticket is missing.\n";
}
?>


pg_ident.conf
# MAPNAME    SYSTEM-USERNAME        PG-USERNAME
krbmap       /^[a-zA-Z0-9]+$/       appuser

pg_hba.conf #Add this line before any other generic host entries
# TYPE       DATABASE   USER     ADDRESS          METHOD
hostgssenc   all        all      0.0.0.0/0        gss map=krbmap

Test the Mapping
export KRB5CCNAME=FILE:/tmp/krb5cc_$(id -u)
kinit -k -t /etc/app.keytab your_user@INT.BAR.COM
psql -h your-db-host -U your_user -d your_db
SELECT current_user;






<?php
try {
    $dsn = 'pgsql:host=db.example.com;port=5432;dbname=mydatabase';
    
    // DO NOT pass username/password — Kerberos will handle it via ticket cache
    $pdo = new PDO($dsn, null, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);

    echo "✅ Kerberos-authenticated connection successful!";
} catch (PDOException $e) {
    echo "❌ Connection failed: " . $e->getMessage();
}


<?php
class KerberosPostgresPDO {
    private $host;
    private $port;
    private $database;
    private $pdo;

    public function __construct($host, $port, $database) {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
    }

    /**
     * Authenticate and establish Kerberos PDO connection
     * @return PDO
     * @throws Exception
     */
    public function connectWithKerberos() {
        // Prerequisite: Ensure Kerberos libraries are installed
        // Install: 
        // - libkrb5-dev 
        // - php-gssapi 
        // - php-pdo-pgsql

        // Step 1: Validate Kerberos ticket
        if (!$this->validateKerberosTicket()) {
            throw new Exception("Kerberos authentication failed");
        }

        // Step 2: Construct PDO connection string with GSSAPI
        $dsn = "pgsql:host={$this->host};port={$this->port};dbname={$this->database};";
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_TIMEOUT => 10,
            PDO::PGSQL_ATTR_USE_GSSAPI => true  // Critical for Kerberos
        ];

        try {
            // Establish PDO connection using Kerberos
            $this->pdo = new PDO($dsn, '', '', $options);
            return $this->pdo;
        } catch (PDOException $e) {
            error_log("Kerberos PDO Connection Error: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Validate existing Kerberos ticket
     * @return bool
     */
    private function validateKerberosTicket(): bool {
        // Check if Kerberos ticket is valid
        $klist = shell_exec('klist -s');
        
        // If no ticket exists or is invalid
        if ($klist === null) {
            // Attempt to renew or acquire ticket
            $kinit = shell_exec('kinit -R');
            
            // If renewal fails, prompt for manual ticket acquisition
            if ($kinit === null) {
                error_log("No valid Kerberos ticket. Please run 'kinit'");
                return false;
            }
        }

        return true;
    }

    /**
     * Execute a prepared statement with Kerberos authentication
     * @param string $query
     * @param array $params
     * @return PDOStatement
     */
    public function executeQuery(string $query, array $params = []) {
        try {
            // Ensure connection is established
            if (!$this->pdo) {
                $this->connectWithKerberos();
            }

            $stmt = $this->pdo->prepare($query);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            error_log("Kerberos Query Execution Error: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Retrieve user principal from Kerberos ticket
     * @return string|null
     */
    public function getCurrentUserPrincipal(): ?string {
        $principal = shell_exec('klist | grep "Principal:" | awk \'{print $2}\'');
        return trim($principal);
    }
}

// Usage Example
try {
    $kerberosDB = new KerberosPostgresPDO(
        'your_postgres_host', 
        5432, 
        'your_database_name'
    );

    // Establish Kerberos-authenticated connection
    $pdo = $kerberosDB->connectWithKerberos();

    // Get current Kerberos user
    $currentUser = $kerberosDB->getCurrentUserPrincipal();
    echo "Authenticated as: " . $currentUser;

    // Execute a sample query
    $stmt = $kerberosDB->executeQuery(
        "SELECT * FROM users WHERE username = :username", 
        ['username' => $currentUser]
    );
    $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

} catch (Exception $e) {
    // Handle authentication or connection errors
    die("Authentication Failed: " . $e->getMessage());
}
