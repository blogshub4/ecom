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
