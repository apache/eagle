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

################################################################
#                    Check Installation                        #
################################################################

echo "Checking required service installation ..."
if [ -z "$(command -v hbase version)" ]
then
	echo 'please make sure the user has the privilege to run HBase shell'
	exit 1
fi

if [ -z "$(command -v storm version)" ]
then
	echo 'please make sure the user has the privilege to run storm'
	exit 1
fi

if [ -z "$(command -v hadoop version)" ]
then
	echo 'please make sure the user has the privilege to run hadoop shell'
	exit 1
fi

echo "Hbase & Storm & Hadoop are installed!"


