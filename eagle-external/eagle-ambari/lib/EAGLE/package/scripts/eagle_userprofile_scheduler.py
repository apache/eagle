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

import sys

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import get_unique_id_and_date
from resource_management.libraries.functions.version import compare_versions, format_hdp_stack_version
from resource_management.libraries.functions.security_commons import build_expectations, \
  cached_kinit_executor, get_params_from_filesystem, validate_security_config_properties, \
  FILE_TYPE_JAAS_CONF
from resource_management.core.shell import call
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.validate import call_and_match_output

from actions import *

class EagleUserProfileScheduler(Script):
  # def get_stack_to_component(self):
  #  return {"HDP": "EAGLE-TOPOLOGY"}

  def install(self, env):
    Logger.info('Install Eagle UserProfile Scheduler')
    # self.install_packages(env)
    import params
    env.set_params(params)
    self.configure(env)
    # eagle_topology_exec(action = 'init')

  def configure(self,env):
    Logger.info("Configure Eagle UserProfile Scheduler")
    import params
    env.set_params(params)

  def pre_rolling_restart(self,env):
    Logger.info("Executing rolling pre-restart Eagle UserProfile Scheduler")
    import params
    env.set_params(params)

    # if params.version and compare_versions(format_hdp_stack_version(params.version), '2.2.0.0') >= 0:
    #  Execute(format("hdp-select set eagle-topology {version}"))

  def stop(self, env):
    Logger.info('Stopping Eagle UserProfile Scheduler')
    import params
    env.set_params(params)
    self.configure(env)
    eagle_userprofile_scheduler_exec(action = 'stop')

  def start(self, env):
    Logger.info('Starting Eagle UserProfile Scheduler')
    import params
    env.set_params(params)
    self.configure(env)
    eagle_userprofile_scheduler_exec(action = 'start')

  def status(self, env):
    Logger.info('Checking Eagle UserProfile Scheduler')
    import params
    env.set_params(params)
    self.configure(env)
    eagle_userprofile_scheduler_exec(action = 'status')

if __name__ == "__main__":
    EagleUserProfileScheduler().execute()