# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### refer to http://mojo.codehaus.org/exec-maven-plugin/usage.html
### java goal is to execute program within maven JVM
export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)
###mvn -X exec:java  -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 10000000" -PhostIdentifier
mvn -X exec:java  -Dexec.mainClass="eagle.app.security.dataproc.util.AuditLogIP2HostIdentifier" -Dexec.args="-input /Users/user1/Downloads/hdfs-audit.log.2015-01-22-22 -maxNum 10000000"
