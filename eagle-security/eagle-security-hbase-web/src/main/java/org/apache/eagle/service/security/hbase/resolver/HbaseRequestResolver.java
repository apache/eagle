/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.service.security.hbase.resolver;


import org.apache.eagle.security.resolver.AbstractCommandResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HbaseRequestResolver extends AbstractCommandResolver {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseRequestResolver.class);
    private final String [] master = {"createTable", "modifyTable", "deleteTable", "truncateTable", "addColumn", "modifyColumn",
                                        "deleteColumn", "enableTable", "disableTable", "disableAclTable", "move", "assign", "unassign",
                                        "regionOffline", "balance", "balanceSwitch", "shutdown", "stopMaster", "snapshot",
                                        "listSnapshot", "cloneSnapshot", "restoreSnapshot", "deleteSnapshot", "createNamespace",
                                        "deleteNamespace", "modifyNamespace", "getNamespaceDescriptor", "listNamespaceDescriptors*",
                                        "flushTable", "getTableDescriptors*", "getTableNames*", "setUserQuota", "setTableQuota",
                                        "setNamespaceQuota"};
    private final String [] region = {"openRegion", "closeRegion", "flush", "split", "compact", "getClosestRowBefore", "getOp", "exists",
                                        "put", "delete", "batchMutate", "checkAndPut", "checkAndPutAfterRowLock", "checkAndDelete",
                                        "checkAndDeleteAfterRowLock", "incrementColumnValue", "append", "appendAfterRowLock","increment",
                                        "incrementAfterRowLock", "scan", "bulkLoadHFile", "prepareBulkLoad",
                                        "cleanupBulkLoad"};
    private final String [] endpoint= {"invoke"};
    private final String [] accessController = {"grant", "revoke", "getUserPermissions"};
    private final String [] regionServer = {"stopRegionServer", "mergeRegions", "rollWALWriterRequest", "replicateLogEntries"};

    @Override
    public void init(){
        List<String> cmds = new ArrayList<>();
        cmds.addAll(Arrays.asList(master));
        cmds.addAll(Arrays.asList(region));
        cmds.addAll(Arrays.asList(regionServer));
        cmds.addAll(Arrays.asList(endpoint));
        cmds.addAll(Arrays.asList(accessController));
        this.setCommands(cmds);
    }

}

