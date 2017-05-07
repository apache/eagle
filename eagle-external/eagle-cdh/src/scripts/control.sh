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

CMD=$1

cp -r /opt/cloudera/parcels/EAGLE/* $CONF_DIR
mv -f $CONF_DIR/eagle.properties $CONF_DIR/conf

case $CMD in
  (start)
    echo "Starting the web server on port [$WEBSERVER_PORT]"
    cmd="bin/eagle-server.sh start"
    exec ${cmd}
    ;;
  (stop)
    pkill -u eagle -f ServerMain
    ;;
  (*)
    echo "Don't understand [$CMD]"
    exit 1
    ;;
esac
