/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.health.jobs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.app.job.MonitorJob;
import org.apache.eagle.app.job.MonitorResult;
import org.apache.eagle.health.EventType;
import org.apache.eagle.health.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class HBaseHealthCheckJob extends MonitorJob implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseHealthCheckJob.class);
    private static final String CONNECTION_PROPS_KEY = "connectionProps";
    private static final String TABLE_NAME_KEY = "tableName";
    private static final String ALWAYS_CREATE_TABLE_KEY = "alwaysCreateTable";

    private static final String DEFAULT_TABLE_NAME = "eagle_healthcheck";

    private String tableNameStr;
    private HBaseAdmin hbaseAdmin;
    private ZooKeeper zookeeper;
    private Configuration conf;
    private TableName tableName;
    // private HTable table;
    private static final byte[] familyBytes = Bytes.toBytes("f");
    private static final byte[] qualifierBytes = Bytes.toBytes("q");
    private static final byte[] valueBytes = Bytes.toBytes("v");
    private List<String> regionServers;
    private List<HRegionLocation> regionLocations;
    private boolean alwaysCreateTable = false;

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString());
    }

    @Override
    protected boolean isParallelTriggerAllowed() {
        return false;
    }

    @Override
    protected void prepare(JobExecutionContext context) throws JobExecutionException {
        try {
            Properties connectionProps = new Properties();
            if (context.getMergedJobDataMap().containsKey((CONNECTION_PROPS_KEY))) {
                connectionProps.load(new StringReader(context.getMergedJobDataMap().getString(CONNECTION_PROPS_KEY)));
            }
            if (context.getMergedJobDataMap().containsKey(ALWAYS_CREATE_TABLE_KEY)) {
                this.alwaysCreateTable = context.getMergedJobDataMap().getBoolean(ALWAYS_CREATE_TABLE_KEY);
            }
            this.conf = HBaseConfiguration.create();
            this.tableNameStr = context.getMergedJobDataMap().getString(TABLE_NAME_KEY);
            if (StringUtils.isEmpty(this.tableNameStr)) {
                this.tableNameStr = DEFAULT_TABLE_NAME;
            }
            this.tableName = TableName.valueOf(tableNameStr);
            PropertiesUtils.merge(connectionProps, this.conf);
            doConnect();
        } catch (IOException | KeeperException | InterruptedException e) {
            throw new JobExecutionException(e);
        }
    }

    @Override
    protected MonitorResult execute() throws Exception {
        try {
            doCheck();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
        return MonitorResult.ok("HBase is healthy");
    }

    private void deleteTable(HBaseAdmin hbaseAdmin) throws IOException {
        if (hbaseAdmin.tableExists(tableName)) {
            if (hbaseAdmin.isTableEnabled(tableName)) {
                LOG.info("Disable table \"" + tableNameStr + "\"");
                hbaseAdmin.disableTable(tableName);
            }
            LOG.info("Dropping table \"" + tableNameStr + "\"");
            hbaseAdmin.deleteTable(tableName);
        } else {
            LOG.info("Table \"" + tableNameStr + "\" not exists");
        }
    }

    @Override
    protected void close() throws JobExecutionException {
        if (this.regionServers != null) {
            this.regionServers.clear();
        }
        if (this.regionLocations != null) {
            this.regionLocations.clear();
        }

        HBaseAdmin _hbaseAdmin = this.hbaseAdmin;
        ZooKeeper _zookeeper = this.zookeeper;

        this.hbaseAdmin = null;
        this.zookeeper = null;

        try {
            if (_hbaseAdmin != null) {
                try {
                    if (alwaysCreateTable) {
                        deleteTable(_hbaseAdmin);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to disable or drop table", e);
                } finally {
                    LOG.info("Closing hbase admin");
                    _hbaseAdmin.close();
                }
            }

            if (_zookeeper != null) {
                LOG.info("Closing zookeeper");
                _zookeeper.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close hbase clients", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted to close zookeeper client", e);
        } catch (Throwable e) {
            throw new JobExecutionException(e);
        }
    }


    private void doCheck() throws JobExecutionException {
        boolean tableExists;
        try {
            LOG.info("Checking whether hbase table {} exists", tableNameStr);
            tableExists = hbaseAdmin.tableExists(tableName);
        } catch (IOException e) {
            LOG.error("Failed to check whether table exists due to: " + e.getMessage(), e);
            // emit(EventType.HBASE_READ_TABLE_ERROR, e, "Failed to check whether hbase table exists");
            throw new JobExecutionException(e);
        }

        if (tableExists && alwaysCreateTable) {
            LOG.debug("Table {} already exist, dropping", tableNameStr);
            try {
                deleteTable(hbaseAdmin);
            } catch (IOException e) {
                LOG.error("Failed to delete table {}", tableNameStr, e);
            }
            LOG.info("Dropped table {}", tableNameStr);
            createTable();
        }

        HTable table = null;
        try {
            LOG.info("Opening connection to hbase table {}", tableNameStr);
            table = new HTable(conf, tableName);
            table.setOperationTimeout(10000);
            table.setAutoFlushTo(true);
            if (tableExists && !alwaysCreateTable) {
                cleanTable(table);
            }
            assignRegions(table);
            checkRegions(table);
        } catch (IOException e) {
            LOG.error(e.getMessage(),e );
            throw new JobExecutionException("Failed to create hbase table", e);
        } finally {
            if (table != null) {
                try {
                    if (!alwaysCreateTable) {
                        cleanTable(table);
                    }
                    LOG.info("Closing table {}", tableNameStr);
                    table.close();
                } catch (IOException e) {
                    LOG.error("Failed to close table {}", tableNameStr, e);
                }
            }
        }
    }

    private void cleanTable(HTable table) {
        LOG.info("Cleaning up table {}", tableNameStr);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            List<Delete> deleteList = new LinkedList<>();
            scanner.forEach((result -> {
                deleteList.add(new Delete(result.getRow()));
            }));
            table.delete(deleteList);
        } catch (IOException e) {
            LOG.error("Error to clean up table {}", tableNameStr, e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void createTable() throws JobExecutionException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor("f");
        desc.addFamily(family);

        // set hbase.hregion.max.filesize to 1, use add record to trigger split region
        desc.setMaxFileSize(1);
        desc.setConfiguration("hbase.table.sanity.checks", "false");

        byte[][] splits = new byte[regionServers.size() - 1][];
        for (int i = 0; i < regionServers.size() - 1; i++) {
            splits[i] = Bytes.toBytes(Integer.toString(10 * (i + 1)));
        }

        try {
            hbaseAdmin.createTable(desc, splits); //Creates a new table with an initial set of empty regions defined by the specified split keys.
        } catch (TableExistsException e) {
            LOG.warn("Table {} already exists", tableNameStr);
        } catch (IOException e) {
            LOG.error("Failed to create hbase table {}", tableNameStr, e);
            throw new JobExecutionException(EventType.HBASE_CREATE_TABLE_ERROR + ": Failed to create hbase table " + Arrays.toString(tableName.getName()), e);
        }

        LOG.info("Table " + tableNameStr + " created.");
    }

    private static final Integer VALUE_INTERVAL = 10;

    private boolean assignRegions(HTable table) throws JobExecutionException {
        LOG.info("Assigning regions");

        ListMultimap<String, String> host2Regions = loadHostRegionsMapping();

        LOG.info("Current regionServerNum = " + regionServers.size() + " and regionNum = " + host2Regions.size());

        final int num = regionServers.size() - host2Regions.size();

        if (num > 0) {
            try {
                LOG.debug("Creating " + num + " regions");
                createRegions(table, num, host2Regions.size() * VALUE_INTERVAL, VALUE_INTERVAL);
            } catch (IOException e) {
                LOG.error("failed to create regions due to " + e.getMessage(), e);
                throw new JobExecutionException("Failed to create region", e);
            } catch (InterruptedException e) {
                LOG.error("interrupted when to create regions due to " + e.getMessage(), e);
                throw new JobExecutionException("Interrupted when to create regions", e);
            }
            LOG.info("Created " + num + " new regions");
            loadHostRegionsMapping();
        }
        return true;
    }

    private ListMultimap<String, String> loadHostRegionsMapping() throws JobExecutionException {
        LOG.info("Reading regions");
        ListMultimap<String, String> hostRegionsMap = ArrayListMultimap.create();

        try {
            regionLocations = hbaseAdmin.getConnection().locateRegions(tableName);
        } catch (IOException e) {
            LOG.error("Failed to locate region servers' locations of table " + tableNameStr, e);
            throw new JobExecutionException("Failed to locate region servers' locations of table " + tableNameStr, e);
        }

        for (HRegionLocation hrl : regionLocations) {
            hostRegionsMap.put(hrl.getServerName().getHostname(), hrl.getRegionInfo().getEncodedName());
        }

        return hostRegionsMap;
    }

    private void createRegions(HTable table, int numOfRegions, Integer startKey, Integer interval) throws IOException, InterruptedException {
        LOG.info("Add regions with start key: " + startKey);
        int num = 0;
        for (startKey += interval; numOfRegions > 0; numOfRegions--, startKey += interval) {
            Put put = new Put(startKey.toString().getBytes());
            put.add(familyBytes, qualifierBytes, valueBytes);
            table.put(put);
            hbaseAdmin.split(tableNameStr.getBytes(), startKey.toString().getBytes());
            LOG.debug("Add region, startKey is " + startKey.toString());
            num++;
        }
        table.flushCommits();
        LOG.info("Flushing " + num + " rows");
    }

    private void checkRegions(HTable table) throws JobExecutionException {
        LOG.info("Checking PUT operation on {} region severs (one region per region server)", regionLocations.size());
        List<String> failedHosts = new ArrayList<>();
        for (HRegionLocation rl : regionLocations) {
            byte[] startKey = rl.getRegionInfo().getStartKey();
            if (startKey.length < 1) {
                startKey = "0".getBytes();
            }
            Put put = new Put(startKey);
            put.add(familyBytes, qualifierBytes, valueBytes);
            try {
                table.put(put);
                table.flushCommits();
                LOG.info("Successful PUT on region sever {}", rl.getHostnamePort());
            } catch (Exception e) {
                LOG.error("Failed PUT on region server {}", rl.getHostnamePort(), e);
                failedHosts.add(rl.getServerName().getHostname());
            }
        }

        if (failedHosts.size() > 0) {
            LOG.error("Failed to write to {} region servers: {}", failedHosts.size());
            throw new JobExecutionException(String.format("Failed PUT on %s region servers: [%s]", failedHosts.size(), StringUtils.join(failedHosts,",")));
        }
    }

    private void doConnect() throws IOException, KeeperException, InterruptedException {
        LOG.info("Connecting HBase ...");
        try {
            hbaseAdmin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }

        LOG.info("Connected to HBase Master");

        try {
            zookeeper = new ZooKeeper(conf.get("hbase.zookeeper.quorum"), 3000, this);
        } catch (IOException e) {
            LOG.error("Failed to connect to zookeeper", e);
            // emit(EventType.ZOOKEEPER_CONNECTION_ERROR, e, "failed to doConnect to zookeeper");
            throw e;
        }

        LOG.info("Connected to ZooKeeper");

        // Get region server list
        String zkZnodeParent = conf.get("zookeeper.znode.parent", "/hbase");
        String rsZkZnode = zkZnodeParent + "/rs";

        int tryTimes = 0;
        boolean success = false;
        while (!success && tryTimes < 3) {
            try {
                tryTimes++;
                regionServers = zookeeper.getChildren(rsZkZnode, false);
                success = true;
            } catch (Exception e) {
                LOG.error("Got zookeeper exception to get region servers from znode: {}, tried {} times", rsZkZnode, tryTimes, e);
                if (tryTimes >= 3) {
                    throw e;
                }
            }
        }
        LOG.info("Successfully connected to hbase and read {} region servers from zookeeper: {}", regionServers.size(), rsZkZnode);
    }
}