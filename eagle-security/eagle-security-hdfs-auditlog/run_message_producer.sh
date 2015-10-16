export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
### mvn -X exec:java  -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 10000000 -topic hdfs_audit_log" -Pproducer
mvn exec:java -Dexec.mainClass="eagle.security.auditlog.util.AuditLogKafkaProducer" -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 20000 -topic hdfs_audit_log"

