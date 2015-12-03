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

: ${VERSION:=latest}
: ${IMAGE:="apacheeagle/sandbox:${VERSION}"}

: ${NODE_PREFIX:=sandbox}
: ${AMBARI_SERVER_NAME:=${NODE_PREFIX}}
: ${MYDOMAIN:=eagle.incubator.apache.org}
: ${DOCKER_OPTS:="--dns 127.0.0.1 --entrypoint /usr/local/serf/bin/start-serf-agent.sh -e KEYCHAIN=$KEYCHAIN --env EAGLE_SERVER_HOST=${AMBARI_SERVER_NAME}.${MYDOMAIN}"}
: ${CLUSTER_SIZE:=1}
: ${DEBUG:=1}
: ${SLEEP_TIME:=2}
: ${DRY_RUN:=false}

run-command() {
  CMD="$@"
  if [ "$DRY_RUN" == "false" ]; then
    debug "$CMD"
    "$@"
  else
    debug [DRY_RUN] "$CMD"
  fi
}

amb-clean() {
  unset NODE_PREFIX AMBARI_SERVER_NAME MYDOMAIN IMAGE DOCKER_OPTS DEBUG SLEEP_TIME AMBARI_SERVER_IP DRY_RUN
}

get-ambari-server-ip() {
  AMBARI_SERVER_IP=$(get-host-ip ${AMBARI_SERVER_NAME})
}

get-host-ip() {
  HOST=$1
  docker inspect --format="{{.NetworkSettings.IPAddress}}" ${HOST}
}

amb-members() {
  get-ambari-server-ip
  serf members --rpc-addr $(docker inspect --format "{{.NetworkSettings.IPAddress}}" ${AMBARI_SERVER_NAME}):7373
}

amb-settings() {
  cat <<EOF
  NODE_PREFIX=$NODE_PREFIX
  MYDOMAIN=$MYDOMAIN
  CLUSTER_SIZE=$CLUSTER_SIZE
  AMBARI_SERVER_NAME=$AMBARI_SERVER_NAME
  IMAGE=$IMAGE
  DOCKER_OPTS=$DOCKER_OPTS
  AMBARI_SERVER_IP=$AMBARI_SERVER_IP
  DRY_RUN=$DRY_RUN
EOF
}

debug() {
  [ $DEBUG -gt 0 ] && echo [DEBUG] "$@" 1>&2
}

docker-ps() {
  #docker ps|sed "s/ \{3,\}/#/g"|cut -d '#' -f 1,2,7|sed "s/#/\t/g"
  docker inspect --format="{{.Name}} {{.NetworkSettings.IPAddress}} {{.Config.Image}} {{.Config.Entrypoint}} {{.Config.Cmd}}" $(docker ps -q)
}

docker-psa() {
  #docker ps|sed "s/ \{3,\}/#/g"|cut -d '#' -f 1,2,7|sed "s/#/\t/g"
  docker inspect --format="{{.Name}} {{.NetworkSettings.IPAddress}} {{.Config.Image}} {{.Config.Entrypoint}} {{.Config.Cmd}}" $(docker ps -qa)
}

amb-start-cluster() {
  local act_cluster_size=$1
  : ${act_cluster_size:=$CLUSTER_SIZE}
  echo starting an ambari cluster with: $act_cluster_size nodes

  amb-start-first
  [ $act_cluster_size -gt 1 ] && for i in $(seq $((act_cluster_size - 1))); do
    amb-start-node $i
  done
}

_amb_run_shell() {
  COMMAND=$1
  : ${COMMAND:? required}
  get-ambari-server-ip
  NODES=$(docker inspect --format="{{.Config.Image}} {{.Name}}" $(docker ps -q)|grep $IMAGE|grep $NODE_PREFIX|wc -l|xargs)
  run-command docker run -it --rm -e EXPECTED_HOST_COUNT=$NODES -e BLUEPRINT=$BLUEPRINT --link ${AMBARI_SERVER_NAME}:ambariserver --entrypoint /bin/sh $IMAGE -c $COMMAND
}

amb-shell() {
  _amb_run_shell /tmp/ambari-shell.sh
}

eagle-deploy-cluster() {
  local act_cluster_size=$1
  : ${act_cluster_size:=$CLUSTER_SIZE}

  if [ $# -gt 1 ]; then
    BLUEPRINT=$2
  else
    [ $act_cluster_size -gt 1 ] && BLUEPRINT=hdp-multinode-eagle || BLUEPRINT=hdp-singlenode-eagle
  fi

  : ${BLUEPRINT:?" required (hdp-singlenode-eagle / hdp-multinode-eagle)"}

  amb-start-cluster $act_cluster_size
  _amb_run_shell /tmp/install-cluster.sh
}

amb-start-first() {
  run-command docker run -p 9099:9099 -p 8080:8080 -p 8744:8744 -p 2181:2181 -p 2888:2888 -p 6667:6667 -p 60020:60020 -p 60030:60030 -p 60010:60010 -d $DOCKER_OPTS --name $AMBARI_SERVER_NAME -h $AMBARI_SERVER_NAME.$MYDOMAIN --privileged=true $IMAGE --tag ambari-server=true
}

amb-copy-to-hdfs() {
  get-ambari-server-ip
  FILE_PATH=${1:?"usage: <FILE_PATH> <NEW_FILE_NAME_ON_HDFS> <HDFS_PATH>"}
  FILE_NAME=${2:?"usage: <FILE_PATH> <NEW_FILE_NAME_ON_HDFS> <HDFS_PATH>"}
  DIR=${3:?"usage: <FILE_PATH> <NEW_FILE_NAME_ON_HDFS> <HDFS_PATH>"}
  amb-create-hdfs-dir $DIR
  DATANODE=$(curl -si -X PUT "http://$AMBARI_SERVER_IP:50070/webhdfs/v1$DIR/$FILE_NAME?user.name=hdfs&op=CREATE" |grep Location | sed "s/\..*//; s@.*http://@@")
  DATANODE_IP=$(get-host-ip $DATANODE)
  curl -T $FILE_PATH "http://$DATANODE_IP:50075/webhdfs/v1$DIR/$FILE_NAME?op=CREATE&user.name=hdfs&overwrite=true&namenoderpcaddress=$AMBARI_SERVER_IP:8020"
}

amb-create-hdfs-dir() {
  get-ambari-server-ip
  DIR=$1
  curl -X PUT "http://$AMBARI_SERVER_IP:50070/webhdfs/v1$DIR?user.name=hdfs&op=MKDIRS" > /dev/null 2>&1
}

amb-scp-to-first() {
  get-ambari-server-ip
  FILE_PATH=${1:?"usage: <FILE_PATH> <DESTINATION_PATH>"}
  DEST_PATH=${2:?"usage: <FILE_PATH> <DESTINATION_PATH>"}
  scp $FILE_PATH root@$AMBARI_SERVER_IP:$DEST_PATH
}

amb-start-node() {
  get-ambari-server-ip
  : ${AMBARI_SERVER_IP:?"AMBARI_SERVER_IP is needed"}
  NUMBER=${1:?"please give a <NUMBER> parameter it will be used as node<NUMBER>"}
  if [ $# -eq 1 ] ;then
    MORE_OPTIONS="-d"
  else
    shift
    MORE_OPTIONS="$@"
  fi
  run-command docker run $MORE_OPTIONS -e SERF_JOIN_IP=$AMBARI_SERVER_IP $DOCKER_OPTS --name ${NODE_PREFIX}_$NUMBER -h ${NODE_PREFIX}_${NUMBER}.$MYDOMAIN $IMAGE --log-level debug
}
