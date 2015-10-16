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

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.validate import call_and_match_output
from resource_management.libraries.functions.format import format
from resource_management.core.logger import Logger

class EagleServiceCheck(Script):
  def service_check(self,env):
    Logger.info("Checking eagle service") 
    import params
    env.set_params(params)
    check_eagle_service_cmd=format("ls {eagle_service_pid_file} >/dev/null 2>&1 && ps -p `cat {eagle_service_pid_file}` >/dev/null 2>&1")
    Execute(check_eagle_service_cmd,logoutput=True,try_sleep=3, tries=5)

if __name__ == "__main__":
  EagleServiceCheck().execute()
