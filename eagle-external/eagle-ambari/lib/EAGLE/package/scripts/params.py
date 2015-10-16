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
Ambari Agent
"""

from resource_management.libraries.functions.version import format_hdp_stack_version, compare_versions
from resource_management.libraries.functions.default import default
from resource_management import *
# import status_params

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
hdp_stack_version = format_hdp_stack_version(stack_version_unformatted)

stack_name = default("/hostLevelParams/stack_name", None)

# New Cluster Stack Version that is defined during the RESTART of a Rolling Upgrade
version = default("/commandParams/version", None)

#hadoop params
if hdp_stack_version != "" and compare_versions(hdp_stack_version, '2.2') >= 0:
  role_root = "eagle"
  command_role = default("/role", "")

  if command_role == "EAGLE_SERVICE":
    role_root = "eagle"

  eagle_home = format("/usr/hdp/current/{role_root}")
  eagle_bin = format("/usr/hdp/current/{role_root}/bin")
  eagle_conf = format("/usr/hdp/current/{role_root}/conf")
else:
  eagle_home = "/usr"
  eagle_bin = "/usr/lib/zookeeper/bin"

eagle_user =  'root'
eagle_site = 'sandbox'
eagle_service_pid_file = format("{eagle_home}/temp/service.pid")