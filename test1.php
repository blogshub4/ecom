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

