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

# Eagle - Git Ignore Configuration
#
# See: https://github.com/github/gitignore/

# Versions available for testing via binary distributions
OFFICIAL_RELEASES="0.8.0 0.8.1 0.8.1.1 0.8.2.0"

# Useful configuration vars, with sensible defaults
if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.10
fi

# On travis CI, empty KAFKA_VERSION means skip integration tests
# so we dont try to get binaries 
# Otherwise it means test all official releases, so we get all of them!
if [ -z "$KAFKA_VERSION" -a -z "$TRAVIS" ]; then
  KAFKA_VERSION=$OFFICIAL_RELEASES
fi

# By default look for binary releases at archive.apache.org
if [ -z "$DIST_BASE_URL" ]; then
  DIST_BASE_URL="https://archive.apache.org/dist/kafka/"
fi

# When testing against source builds, use this git repo
if [ -z "$KAFKA_SRC_GIT" ]; then
  KAFKA_SRC_GIT="https://github.com/apache/kafka.git"
fi

pushd servers
  mkdir -p dist
  pushd dist
    for kafka in $KAFKA_VERSION; do
      if [ "$kafka" == "trunk" ]; then
        if [ ! -d "$kafka" ]; then
          git clone $KAFKA_SRC_GIT $kafka
        fi
        pushd $kafka
          git pull
          ./gradlew -PscalaVersion=$SCALA_VERSION -Pversion=$kafka releaseTarGz -x signArchives
        popd
        # Not sure how to construct the .tgz name accurately, so use a wildcard (ugh)
        tar xzvf $kafka/core/build/distributions/kafka_*.tgz -C ../$kafka/
        rm $kafka/core/build/distributions/kafka_*.tgz
        mv ../$kafka/kafka_* ../$kafka/kafka-bin
      else
        echo "-------------------------------------"
        echo "Checking kafka binaries for ${kafka}"
        echo
        # kafka 0.8.0 is only available w/ scala 2.8.0
        if [ "$kafka" == "0.8.0" ]; then
          KAFKA_ARTIFACT="kafka_2.8.0-${kafka}"
        else
          KAFKA_ARTIFACT="kafka_${SCALA_VERSION}-${kafka}"
        fi
        wget -N https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tgz || wget -N https://archive.apache.org/dist/kafka/$kafka/${KAFKA_ARTIFACT}.tar.gz
        echo
        if [ ! -d "../$kafka/kafka-bin" ]; then
          echo "Extracting kafka binaries for ${kafka}"
          tar xzvf ${KAFKA_ARTIFACT}.t* -C ../$kafka/
          mv ../$kafka/${KAFKA_ARTIFACT} ../$kafka/kafka-bin
        else
          echo "$kafka/kafka-bin directory already exists -- skipping tgz extraction"
        fi
      fi
      echo
    done
  popd
popd
