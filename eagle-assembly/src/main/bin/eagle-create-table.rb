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
    # create tableName, {NAME => 'f', VERSIONS => '1', BLOOMFILTER => 'ROW', COMPRESSION => 'GZ'}
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

if ARGV.empty?
  puts "Table list is empty, please go back to bin/eagle-env.sh and export EAGLE_TABLE_LIST"
  exit 1
end

tableListVal=ARGV.first

tableListVal.split(' ').map { |i| createEagleTable(admin, i) }

exit 0
