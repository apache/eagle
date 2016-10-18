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
#
# Continuous Integration
# Go to eagle project root directory

cd "$(dirname $0)/../"

# Check whether COVERALLS_EAGLE_TOKEN is set
if [ -z "$COVERALLS_EAGLE_TOKEN" ];then
    echo "Error: COVERALLS_EAGLE_TOKEN is not set, get token from https://coveralls.io/github/apache/incubator-eagle" 1>&2
    exit 1
fi

# build and report to coveralls
mvn clean test cobertura:cobertura coveralls:report -DrepoToken=$COVERALLS_EAGLE_TOKEN -Dmaven.javadoc.skip=true -P!ui
echo "Check report at https://coveralls.io/github/apache/incubator-eagle"
