#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Eagle Ambari Command
"""
from resource_management import *

def eagle_service_exec(action="start"):
  import params

  eagle_service_init_shell = format("{eagle_bin}/eagle-service-init.sh")
  eagle_service_shell = format("{eagle_bin}/eagle-service.sh")

  if action == "start":
    start_cmd=format("{eagle_service_shell} start")
    Execute(start_cmd, user=params.eagle_user)
  elif action == "stop":
    stop_cmd=format("{eagle_service_shell} stop")
    Execute(stop_cmd,user=params.eagle_user)
  elif action == "status":
    status_cmd=format("{eagle_service_shell} status")
    Execute(status_cmd,user=params.eagle_user)
  elif action == "init":
    Execute(eagle_service_init_shell,user=params.eagle_user)
  else:
    raise Exception('Unknown eagle service action: '+action)

def eagle_topology_exec(action="start"):
  import params

  eagle_topology_shell = format("{eagle_bin}/eagle-topology.sh")
  eagle_topology_init = format("{eagle_bin}/eagle-topology-init.sh")

  if action == "start":
    start_cmd = format("{eagle_topology_shell} start")
    Execute(start_cmd, user=params.eagle_user)
  elif action == "stop":
    stop_cmd = format("{eagle_topology_shell} stop")
    Execute(stop_cmd, user=params.eagle_user)
  elif action == "status":
    status_cmd = format("{eagle_topology_shell} --storm-ui status")
    Execute(status_cmd, user=params.eagle_user)
  elif action == "init":
    Execute(eagle_topology_init, user=params.eagle_user)
  else:
    raise Exception('Unknown eagle topology action: '+action)

def eagle_hive_topology_exec(action="start"):
    import params

    main_class="org.apache.eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain"
    topology_name=format("{eagle_site}-hiveQueryRunningTopology")
    config_file=format("{eagle_conf}/{eagle_site}-hiveQueryLog-application.conf")
    eagle_topology_shell=format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init=format("{eagle_bin}/eagle-topology-init.sh")

    cmd=None

    if action == "start":
        cmd = format("{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format("{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknown eagle hive topology action: '+action)

def eagle_hdfs_topology_exec(action="start"):
    import params

    main_class="org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain"
    topology_name=format("{eagle_site}-hdfsAuditLog-topology")
    config_file=format("{eagle_conf}/{eagle_site}-hdfsAuditLog-application.conf")

    eagle_topology_shell=format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init=format("{eagle_bin}/eagle-topology-init.sh")

    cmd=None
    if action == "start":
        cmd = format("{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format("{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknow eagle hdfs topology action: '+action)

def eagle_userprofile_topology_exec(action="start"):
    import params

    main_class="org.apache.eagle.security.userprofile.UserProfileDetectionMain"
    topology_name=format("{eagle_site}-userprofile-topology")
    config_file=format("{eagle_conf}/{eagle_site}-userprofile-topology.conf")

    eagle_topology_shell=format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init=format("{eagle_bin}/eagle-topology-init.sh")

    cmd=None
    if action == "start":
        cmd = format("{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format("{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknow eagle userprofile topology action: '+action)

def eagle_userprofile_scheduler_exec(action = "start"):
    import params
    userprofile_scheduler_shell=format("{eagle_bin}/eagle-userprofile-scheduler.sh --site {params.eagle_site}")
    if action == "start":
        Execute(format("{userprofile_scheduler_shell} --daemon {action}"), user=params.eagle_user)
    elif action == "stop" or action == "status":
        Execute(format("{userprofile_scheduler_shell} {action}"), user=params.eagle_user)
    else:
        raise Exception("known eagle user profile scheduler action: "+action)
