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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class DistributedFileSystemMetadata extends DistributedFileSystem {

    private static final Logger LOG = Logger.getLogger(DistributedFileSystemMetadata.class);

    private static final int MAX_NUMBER_OF_LOCATIONS = 20000;

    public DistributedFileSystemMetadata(Configuration conf) throws IOException {
        initialize(getDefaultUri(conf), conf);
    }

    public String[] getDataNodes() {
        try {
            DatanodeInfo[] dataNodeStats = getDataNodeStats();
            String[] hosts = new String[dataNodeStats.length];
            for (int i = 0; i < hosts.length; i++) {
                hosts[i] = dataNodeStats[i].getHostName();
            }
            return hosts;
        } catch (IOException e) {
            LOG.warn("list of data nodes could not be got from API (requieres higher privilegies).");
        }

        try {
            LOG.warn("getting datanode list from configuration file (may contain data nodes which are not active).");
            return getDataNodesFromConf();
        } catch (IOException e) {
            LOG.warn(e.getMessage());
        }

        LOG.warn("No list of data nodes found");

        return new String[0];
    }

    private String[] getDataNodesFromConf() throws IOException {
        InputStream in = getClass().getResourceAsStream("/dfs.includes");
        if (in == null) {
            throw new IOException("File dfs.includes not found in classpath");
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        LinkedList<String> hostnames = new LinkedList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            hostnames.add(line);
        }

        reader.close();

        return hostnames.toArray(new String[hostnames.size()]);
    }

    /**
     * For clusters with same configuration this method is fine.
     * <p>
     * It gets the list of data directories from local host
     *
     * @return Array of HDFS data directories
     */
    public String[] getDataDirs() {
        String dataDirsParam = getConf().get("dfs.datanode.data.dir");

        if (dataDirsParam == null) {
            LOG.warn("dfs.data.dir or dfs.datanode.data.dir cofiguration parameter is not set, so data directories and number of disk are unknown");

            return null;
        } else {
            return dataDirsParam.split(",");
        }
    }

    public HashMap<String, Integer> getNumberOfDataDirsPerHost() {
        HashMap<String, Integer> disksPerHost = new HashMap<>();

        try {
            @SuppressWarnings("resource")
            DFSClient dfsClient = new DFSClient(NameNode.getAddress(getConf()), getConf());

            DatanodeStorageReport[] datanodeStorageReports = dfsClient.getDatanodeStorageReport(DatanodeReportType.ALL);

            for (DatanodeStorageReport datanodeStorageReport : datanodeStorageReports) {
                disksPerHost.put(
                    datanodeStorageReport.getDatanodeInfo().getHostName(),
                    datanodeStorageReport.getStorageReports().length);

            }
        } catch (IOException e) {
            LOG.warn("number of data directories (disks) per node could not be collected (requieres higher privilegies).");
        }

        return disksPerHost;
    }

    public static HashMap<String, HashMap<Integer, Integer>> computeHostsDiskIdsCount(
        List<BlockLocation> blockLocations) throws IOException {

        HashMap<String, HashMap<Integer, Integer>> hosts_diskIds = new HashMap<>();
        for (BlockLocation blockLocation : blockLocations) {
            String[] hosts = blockLocation.getHosts();

            VolumeId[] volumeIds = null;
            if (blockLocation instanceof BlockStorageLocation) {
                volumeIds = ((BlockStorageLocation) blockLocation).getVolumeIds();
            }

            for (int i = 0; i < hosts.length; i++) {
                String host = hosts[i];
                Integer diskId = getDiskId(volumeIds != null ? volumeIds[i] : null);

                if (!hosts_diskIds.containsKey(host)) {
                    HashMap<Integer, Integer> diskIds = new HashMap<>();
                    diskIds.put(diskId, 1);
                    hosts_diskIds.put(host, diskIds);
                } else {
                    HashMap<Integer, Integer> diskIds = hosts_diskIds.get(host);
                    Integer count = diskIds.get(diskId);
                    if (count != null) {
                        diskIds.put(diskId, count + 1);
                    } else {
                        diskIds.put(diskId, 1);
                    }
                }
            }
        }

        return hosts_diskIds;
    }

    public LinkedList<BlockLocation> getBlockLocations(Path path) throws IOException {
        LOG.info("Collecting block locations...");

        LinkedList<BlockLocation> blockLocations = new LinkedList<BlockLocation>();
        RemoteIterator<LocatedFileStatus> statuses = listFiles(path, true);
        int hasNextCode = hasNextCode(statuses);
        while (hasNextCode > 0) {
            if (hasNextCode > 1) {
                hasNextCode = hasNextCode(statuses);
                continue;
            }

            LocatedFileStatus fileStatus = statuses.next();

            if (fileStatus.isFile()) {
                BlockLocation[] blockLocations_tmp = getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
                blockLocations.addAll(Arrays.asList(blockLocations_tmp));
            }

            int size = blockLocations.size();
            if (size > 0 && size % 5000 == 0) {
                LOG.info("Collected " + size + " locations. Still in progress...");
            }

            if (size >= MAX_NUMBER_OF_LOCATIONS) {
                LOG.info("Reached max number of locations to collect. The amount will be representative enough.");
                break;
            }

            hasNextCode = hasNextCode(statuses);
        }

        LOG.info("Collected " + blockLocations.size() + " locations.");

        if (isHdfsBlocksMetadataEnabled()) {
            BlockStorageLocation[] blockStorageLocations = getFileBlockStorageLocations(blockLocations);

            blockLocations.clear();
            blockLocations.addAll(Arrays.asList(blockStorageLocations));
        } else {
            LOG.error("VolumnId/DiskId can not be collected since "
                + "dfs.datanode.hdfs-blocks-metadata.enabled is not enabled.");
        }

        return blockLocations;
    }

    private int hasNextCode(RemoteIterator<LocatedFileStatus> statuses) throws IOException {
        try {
            if (statuses.hasNext()) {
                return 1;
            } else {
                return 0;
            }
        } catch (AccessControlException e) {
            String message = e.getMessage();

            LOG.warn("Skipped file or directory because: " + message.substring(0, message.indexOf("\n")));

            return 2;
        }
    }

    public boolean isHdfsBlocksMetadataEnabled() {
        return getConf().getBoolean("dfs.datanode.hdfs-blocks-metadata.enabled", false);
    }

    /**
     * Returns a disk id (0-based) index from the Hdfs VolumeId object. There is
     * currently no public API to get at the volume id. We'll have to get it by
     * accessing the internals.
     */
    public static int getDiskId(VolumeId hdfsVolumeId) {
        // Initialize the diskId as -1 to indicate it is unknown
        int diskId = -1;

        if (hdfsVolumeId != null) {
            String volumeIdString = hdfsVolumeId.toString();

            byte[] volumeIdBytes = StringUtils.hexStringToByte(volumeIdString);
            if (volumeIdBytes != null && volumeIdBytes.length == 4) {
                diskId = Utils.toInt(volumeIdBytes);
            } else if (volumeIdBytes.length == 1) {
                diskId = (int) volumeIdBytes[0];  // support hadoop-2.0.2
            }
        }

        return diskId;
    }

}