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
package org.apache.eagle.servicecheck.jobs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.eagle.app.check.HealthCheckJob;
import org.apache.eagle.app.check.HealthCheckResult;
import org.apache.eagle.metadata.model.AlertEntity;
import org.apache.eagle.servicecheck.EventType;
import org.apache.eagle.servicecheck.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

public class HBaseHealthCheckJob extends HealthCheckJob implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseHealthCheckJob.class);
    private static final String CONNECTION_PROPS_KEY = "connectionProps";

    private String tableNameStr;
    private HBaseAdmin hbaseAdmin;
    private ZooKeeper zookeeper;
    private Configuration conf;
    private TableName tableName;
    private HTable table;
    private static final byte[] familyBytes = Bytes.toBytes("f");
    private static final byte[] qualifierBytes = Bytes.toBytes("q");
    private static final byte[] valueBytes = Bytes.toBytes("v");
    private List<String> regionServers;
    private List<HRegionLocation> regionLocations;
    private Properties connectionProps;

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info(watchedEvent.toString());
    }

    @Override
    protected void prepare(JobExecutionContext context) throws JobExecutionException {
        try {
            this.connectionProps = new Properties();
            if (context.getMergedJobDataMap().containsKey((CONNECTION_PROPS_KEY))) {
                this.connectionProps.load(new StringReader(context.getMergedJobDataMap().getString(CONNECTION_PROPS_KEY)));
            }
            this.conf = HBaseConfiguration.create();
            this.tableNameStr = PropertiesUtils.getString(connectionProps, "checker.hbase.table", "eagle_hbase_health_checker");
            this.tableName = TableName.valueOf(tableNameStr);
            PropertiesUtils.merge(connectionProps, this.conf);
            connect();
        } catch (IOException | KeeperException | InterruptedException e) {
            throw new JobExecutionException(e);
        }
    }

    @Override
    protected HealthCheckResult execute() {
        try {
            doCheck();
        } catch (Throwable e) {
            LOG.error(e.getMessage(),e);
            return HealthCheckResult.critical(e.getMessage(),e);
        }
        return HealthCheckResult.ok("HBase is healthy");
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
        HTable _table = this.table;

        this.hbaseAdmin = null;
        this.zookeeper = null;
        this.table = null;

        try {
            if (_hbaseAdmin != null) {
                try {
                    if (_hbaseAdmin.tableExists(tableName)) {
                        LOG.info("Disable table \"" + tableNameStr + "\"");
                        _hbaseAdmin.disableTable(tableName);
                        LOG.info("Dropping table \"" + tableNameStr + "\"");
                        _hbaseAdmin.deleteTable(tableName);
                    } else {
                        LOG.info("Table \"" + tableNameStr + "\" not exists");
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to disable or drop table", e);
                }
                LOG.info("Closing hbase admin");
                _hbaseAdmin.close();
            }

            if (_zookeeper != null) {
                LOG.info("Closing zookeeper");
                _zookeeper.close();
            }

            if (_table != null) {
                LOG.info("Closing table");
                _table.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close hbase clients", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted to close zookeeper client", e);
        } catch (Throwable e) {
            throw new JobExecutionException(e);
        }
    }


    protected void emit(long timestamp, String metricName, Map<String, String> tags, double value) {

    }

    protected void emit(EventType type, String message) {

    }

    protected void emit(final EventType type, Throwable throwable, final String message) {

    }

    protected void emit(AlertEntity alertEntity) {

    }

    private void doCheck() throws JobExecutionException {
        boolean tableExists;
        try {
            LOG.info("Checking whether hbase table \"" + tableNameStr + "\" exists");
            tableExists = hbaseAdmin.tableExists(tableName);
        } catch (IOException e) {
            LOG.error("Failed to check whether table exists due to: " + e.getMessage(), e);
            emit(EventType.HBASE_READ_TABLE_ERROR, e, "Failed to check whether hbase table exists");
            throw new JobExecutionException(e);
        }

        if (!tableExists) {
            LOG.debug("Table " + tableNameStr + " does not exist, creating");
            createTable();
            LOG.info("Created table " + tableNameStr);
        } else {
            LOG.debug(tableNameStr + " exists");
        }

        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            throw new JobExecutionException("Failed to create hbase table", e);
        }
        table.setOperationTimeout(60000);
        table.setAutoFlushTo(true);
        assignRegions();
        checkRegions();
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
        } catch (IOException e) {
            LOG.error("Failed to create hbase table " + Arrays.toString(tableName.getName()) + " due to " + e.getMessage(), e);
            // emit(EventType.HBASE_CREATE_TABLE_ERROR, e, "failed to create hbase table \"" + tableName.getName() + "\"");
            throw new JobExecutionException(EventType.HBASE_CREATE_TABLE_ERROR + ": Failed to create hbase table " + Arrays.toString(tableName.getName()), e);
        }

        LOG.info("Table " + tableNameStr + " created.");
    }

    private final static Integer VALUE_INTERVAL = 10;

    private boolean assignRegions() throws JobExecutionException {
        LOG.info("Assigning regions");

        ListMultimap<String, String> host2Regions = getHostRegionsMapping();
        // add more region if len(region) < len(regionServer)

        LOG.info("Current regionServerNum = " + regionServers.size() + " and regionNum = " + host2Regions.size());

        final int num = regionServers.size() - host2Regions.size();

        if (num > 0) {
            try {
                LOG.debug("Creating " + num + " regions");
                addRegions(num, host2Regions.size() * VALUE_INTERVAL, VALUE_INTERVAL);
            } catch (IOException e) {
                LOG.error("failed to create regions due to " + e.getMessage(), e);
                // emit(EventType.HBASE_CREATE_REGION_ERROR, e, "failed to create region");
                throw new JobExecutionException("Failed to create region", e);
            } catch (InterruptedException e) {
                LOG.error("interrupted when to create regions due to " + e.getMessage(), e);
                // emit(EventType.HBASE_CREATE_REGION_ERROR, e, "interrupted when to create regions");
                throw new JobExecutionException("Interrupted when to create regions", e);
            }

            host2Regions = getHostRegionsMapping();
            LOG.info("Created " + num + " regions");
        }

        LOG.info("Moving regions to make sure all region servers have at least 1 region");

        // make sure all region server have at least 1 region
        for (String rs : regionServers) {
            if (host2Regions.get(rs).size() == 0) {
                for (String h : host2Regions.keySet()) {
                    if (host2Regions.get(h).size() > 1) {
                        try {
                            String regionName = host2Regions.get(h).get(0);
                            LOG.info("Moving region: " + regionName + " from region server: " + h + " to region server: " + rs);
                            hbaseAdmin.move(host2Regions.get(h).get(0).getBytes(), rs.getBytes());
                        } catch (UnknownRegionException e) {
                            LOG.error("move unknown region to region server " + h + " due to " + e.getMessage(), e);
                            throw new JobExecutionException(e);
                        } catch (MasterNotRunningException e) {
                            LOG.error("hbase master not running", e);
                            throw new JobExecutionException("hbase master not running", e);
                        } catch (ZooKeeperConnectionException e) {
                            LOG.error("got zookeeper connection exception", e);
                            throw new JobExecutionException("Got zookeeper connection exception", e);
                        } catch (Throwable e) {
                            throw new JobExecutionException("Failed to move regions", e);
                        }

                        // remove <h,host2Regions.get(h).get(0)>
                        host2Regions.remove(h, host2Regions.get(h).get(0));
                        break;
                    }
                }
            }
        }
        return true;
    }

    public ListMultimap<String, String> getHostRegionsMapping() throws JobExecutionException {
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

    public void addRegions(int numOfRegions, Integer startKey, Integer interval) throws IOException, InterruptedException {
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

    public void checkRegions() throws JobExecutionException {
        LOG.info("Checking regions ");
        StringBuilder sb = new StringBuilder();
        List<String> failedHosts = new ArrayList<String>();
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
            } catch (Exception e) {
                failedHosts.add(rl.getServerName().getHostname());
                String msg = "Failed writing to region server " + rl.getHostnamePort() + " due to " + e.getMessage();
                sb.append(msg).append("\n");
                LOG.error(msg);
            }
        }

        if (failedHosts.size() > 0) {
            LOG.error("Failed to write to " + failedHosts.size() + " region servers: " + sb.toString());
            emit(EventType.HBASE_WRITE_REGION_ERROR, sb.toString());
            throw new JobExecutionException(sb.toString());
        }
    }

    private void connect() throws IOException, KeeperException, InterruptedException {
        LOG.info("Connecting HBase ...");
        try {
            hbaseAdmin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
            LOG.error(e.getMessage(), e);
            emit(EventType.HBASE_MASTER_NOT_RUNNING, e, "hbase master is not running");
            throw e;
        } catch (ZooKeeperConnectionException e) {
            LOG.error(e.getMessage(), e);
            emit(EventType.ZOOKEEPER_CONNECTION_ERROR, e, "got zookeeper connection exception");
            throw e;
        }

        LOG.info("Connected to HBase Master");

        try {
            zookeeper = new ZooKeeper(conf.get("hbase.zookeeper.quorum"), 3000, this);
        } catch (IOException e) {
            LOG.error("Failed to connect zookeeper", e);
            // emit(EventType.ZOOKEEPER_CONNECTION_ERROR, e, "failed to connect to zookeeper");
            throw e;
        }

        LOG.info("Connected to ZooKeeper");

        // get region server list
        String zkZnodeParent = conf.get("zookeeper.znode.parent","/hbase/rs");

        try {
            regionServers = zookeeper.getChildren(zkZnodeParent, false);
        } catch (KeeperException e) {
            LOG.error("Got zookeeper exception to get region servers from znode: {}",zkZnodeParent, e);
            // emit(EventType.ZOOKEEPER_ERROR, e, "failed to get region servers list from zookeeper" );
            throw e;
        } catch (InterruptedException e) {
            LOG.error("Interrupted to get region servers from znode: {}",zkZnodeParent, e);
            // emit(EventType.ZOOKEEPER_IO_ERROR, e, "interrupted while getting region servers list from from zookeeper");
            throw e;
        }
        LOG.info("Successfully connected to hbase and read {} region servers from zookeeper: {}", regionServers.size(), zkZnodeParent);
    }
}