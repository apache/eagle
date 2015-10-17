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

source $(dirname $0)/eagle-env.sh

function create_policy() {
    echo "Creating Policy $service_name..."
    curl -u ${EAGLE_LOGIN_USER}:${EAGLE_LOGIN_PASSWD} -XPOST -H "Content-Type: application/json" \
        "http://$service_host:$service_port/eagle-service/rest/entities?serviceName=$service_name" \
        -d @$data_file
    exit 0
}


function delete_policy() {
    echo "Deleting policy ..."
    curl -u ${EAGLE_LOGIN_USER}:${EAGLE_LOGIN_PASSWD}  -XDELETE -H "Content-Type: application/json" \
         "http://$service_host:$service_port/eagle-service/rest/entities?query=$service_name[@site=\"$site\" AND @dataSource=\"$source\"]{*}&pageSize=10000"
    if [ $? -eq 0 ]; then
        echo
        echo "Deleting Policy $program_id:$policy_id is completed."
    else
        echo "Error: deleting failed!"
        exit 1
    fi
}

# by default list all policies, or filter with @policyId and so on
function list_policy() {
    if [ -z $site -a -z $dataSource ]; then
        query="http://$service_host:$service_port/eagle-service/rest/list?query=$service_name[]{*}&pageSize=100000"
        echo $query
        curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -XGET --globoff -H "Content-Type: application/json" $query
    else
        query="http://$service_host:$service_port/eagle-service/rest/list?query=$service_name[@site=\"$site\" AND @dataSource=\"$source\"]{*}&pageSize=100000"
        echo $query
        curl -u ${EAGLE_SERVICE_USER}:${EAGLE_SERVICE_PASSWD} -XGET --globoff -H "Content-Type: application/json" $query
    fi
    if [ $? -eq 0 ]; then
        echo
        echo "Listing Policy $service_name is completed."
    else
        echo "Error: listing policy failed!"
        exit 1
    fi
}

function print_help() {
    echo "  Usage: $0 options {create | list}"
    echo "  Options:                     Description:"
    echo "  --host <serviceHost>         eagle service hostname, default is localhost"
    echo "  --port <servicePort>         eagle service port, default is 9099"
    echo "  --serviceName <name>         eagle service name, default is AlertDefinitionService"
    echo "  --site <site>                Default is sandbox"
    echo "  --source <dataSource>        Default is hdfsAuditLog"
    echo "  --file <JsonFile>            policy content"
    echo "  Examples:"
    echo "  createCmd: $0 [--host <serviceHost>] [--port <servicePort>] [--name <service>] --file <datafile> create"
    echo "  listCmd: $0 [--host <serviceHost>] [--port <servicePort>] [--name <service>] [--site <site>] [--source <dataSource>] list"
}

if [ $# -eq 0 ] 
then
	print_help
	exit 1
fi

if [ `expr $# % 2` != 1 ]
then
    print_help
    exit 1
fi

cmd=""
while [  $# -gt 0  ]; do
case $1 in
    "create")
        cmd=$1
        shift
        ;;
    "delete")
        cmd=$1
        shift
        ;;
    "list")
        cmd=$1
        shift
        ;;
     --host)
        service_host=$2
        shift 2
        ;;
    --port)
        service_port=$2
        shift 2
        ;;
    --name)
       service_name=$2
       shift 2
       ;;
    --file)
        data_file=$2
        shift 2
        ;;
    --site)
        site=$2
        shift 2
        ;;
    --source)
        source=$2
        shift 2
        ;;
    *)
        echo "Internal Error: option processing error: $1" 1>&2
        exit 1
        ;;
    esac
done


if [ -z "$service_host" ]; then
    service_host=${EAGLE_SERVICE_HOST}
fi

if [ -z "$service_port" ]; then
    service_port=${EAGLE_SERVICE_PORT}
fi

if [ -z "$service_name" ]; then
    service_name="AlertDefinitionService"
fi

if [ ! -e $data_file ]; then
    echo "Error: json file $data_file is not found!"
    print_help
    exit 1
fi

echo "service_host="$service_host "service_port="$service_port "service_name="$service_name
case $cmd in
"create")
	create_policy
	;;
"list")
	list_policy
	;;
*)
	echo "Invalid command"
	print_help
    exit 1
	;;
esac

exit 0
