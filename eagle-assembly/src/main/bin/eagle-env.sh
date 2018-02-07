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

export EAGLE_SERVER_JMX_PORT=9999

# EAGLE Environment Variables
export EAGLE_HOME=$(readlink -f $(dirname $0)/..)
export EAGLE_CLASSPATH=$EAGLE_HOME/conf

# System Environment Variables
export OS_TYPE="linux"
export CLASSPATH_DELIMITER=":"

case `which uname >/dev/null && uname -s` in
    CYGWIN_NT-* | MINGW64_NT-*)
        OS_TYPE="windows"
        CLASSPATH_DELIMITER=";"
        ;;
    Linux)
        OS_TYPE="linux"
        ;;
   *)
        OS_TYPE="unknown"
        ;;
esac

# Add eagle shared library jars
for file in `ls ${EAGLE_HOME}/lib`; do
	EAGLE_CLASSPATH=${EAGLE_CLASSPATH}${CLASSPATH_DELIMITER}${EAGLE_HOME}/lib/$file
done
