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
class EagleAuditKafka(Script):
    def install(self, env):
        Logger.info('Installing Eagle AuditK2afka Client')
        # self.install_packages(env)
        import params
        env.set_params(params)
        self.configure(env)
        # eagle_topology_exec(action = 'init')

    def configure(self,env):
        Logger.info("Configure Eagle Audit2Kafka Client")
        import params
        env.set_params(params)

    def pre_rolling_restart(self,env):
        Logger.info("Executing rolling pre-restart  Eagle Audit2Kafka Client")
        import params
        env.set_params(params)

        # if params.version and compare_versions(format_hdp_stack_version(params.version), '2.2.0.0') >= 0:
        #  Execute(format("hdp-select set eagle-topology {version}"))

    def stop(self, env):
        Logger.info('Stopping Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        self.configure(env)

    def start(self, env):
        Logger.info('Starting Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        self.configure(env)

    def status(self, env):
        Logger.info('Checking  Eagle Audit2Kafka Client')
        import params
        env.set_params(params)
        self.configure(env)
if __name__ == "__main__":
    EagleAuditKafka().execute()
