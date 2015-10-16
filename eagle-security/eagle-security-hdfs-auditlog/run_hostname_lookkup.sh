### refer to http://mojo.codehaus.org/exec-maven-plugin/usage.html
### java goal is to execute program within maven JVM
export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
###mvn -X exec:java  -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 10000000" -PhostIdentifier
mvn -X exec:java  -Dexec.mainClass="eagle.app.security.dataproc.util.AuditLogIP2HostIdentifier" -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 10000000"
