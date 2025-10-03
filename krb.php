ls -l /etc/*keytab /etc/krb5* /var/lib/pgsql/*.keytab /home/*/*.keytab
find / -name "*.keytab" 2>/dev/null
klist -k -t /path/to/some.keytab
