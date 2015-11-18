# Create HBase tables for Eagle

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

include Java
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

def createEagleTable(admin, tableName)
    if !admin.tableExists(tableName)
        create tableName, {NAME => 'f', VERSIONS => '1', BLOOMFILTER => 'ROW', COMPRESSION => 'SNAPPY'}
	puts "Create Table #{tableName} successfully"
    elsif admin.isTableDisabled(tableName)
        admin.enableTable(tableName)
	  puts "Table #{tableName} already exists"
    else
	  puts "Table #{tableName} already exists"
    end
end

conf = HBaseConfiguration.new
admin = HBaseAdmin.new(conf)

createEagleTable(admin, 'alertdef')
createEagleTable(admin, 'ipzone')
createEagleTable(admin, 'streamMetadata')
createEagleTable(admin, 'alertdetail')
createEagleTable(admin, 'fileSensitivity')
createEagleTable(admin, 'eaglehdfs_alert')
createEagleTable(admin, 'streamdef')
createEagleTable(admin, 'eagle_metric')
createEagleTable(admin, 'alertDataSource')
createEagleTable(admin, 'alertExecutor')
createEagleTable(admin, 'alertStream')
createEagleTable(admin, 'alertStreamSchema')
createEagleTable(admin, 'hiveResourceSensitivity')
createEagleTable(admin, 'hbaseResourceSensitivity')
createEagleTable(admin, 'mlmodel')
createEagleTable(admin, 'userprofile')

exit