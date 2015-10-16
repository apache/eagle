export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
mvn -X exec:java -Dexec.mainClass="eagle.security.auditlog.HdfsAuditLogProcessorMain"
