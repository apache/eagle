#!/bin/bash

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

# set EAGLE_HOME
export EAGLE_HOME=$(dirname $0)/..

# The java implementation to use. please use jdk 1.7 or later
# export JAVA_HOME=${JAVA_HOME}
# export JAVA_HOME=/usr/java/jdk1.7.0_80/

# nimbus.host, default is localhost
export EAGLE_NIMBUS_HOST=localhost

# EAGLE_SERVICE_HOST, default is `hostname -f`
export EAGLE_SERVICE_HOST=localhost

# EAGLE_SERVICE_PORT, default is 9099
export EAGLE_SERVICE_PORT=9099

# EAGLE_SERVICE_USER
export EAGLE_SERVICE_USER=admin

# EAGLE_SERVICE_PASSWORD
export EAGLE_SERVICE_PASSWD=secret

export EAGLE_CLASSPATH=$EAGLE_HOME/conf
# Add eagle shared library jars
for file in $EAGLE_HOME/lib/share/*;do
	EAGLE_CLASSPATH=$EAGLE_CLASSPATH:$file
done
