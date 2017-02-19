/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.health.jobs;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.app.job.MonitorJob;
import org.apache.eagle.app.job.MonitorResult;
import org.apache.eagle.health.entities.HDFSBlockEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@DisallowConcurrentExecution
public class HDFSBlockSnapshotJob extends MonitorJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBlockSnapshotJob.class);
    private static final String HDFS_PROPS_KEY = "connectionProps";
    private static final String SERVICE_HOST_KEY = "serviceHost";
    private static final String SERVICE_PORT_KEY = "servicePort";
    private static final long SNAPSHOT_RENTENTION_MS = 60 * 1000 * 60 * 24;

    private static final Pattern DOT_ONLY_PATTERN = Pattern.compile("^\\.+$");
    private static final Pattern UNDER_REPLICATED_BLOCK_PATTERN =
        Pattern.compile("(.*):\\s+Under replicated (.*):(blk_\\d+)_(\\d+). Target Replicas is (\\d) but found (\\d) replica\\(s\\).");
    private static final Pattern CORRUPT_BLOCK_PATTERN =
        Pattern.compile("(.*): CORRUPT blockpool (.*) block (blk_\\d+)");
    private static final Pattern RACK_REPLICATED_BLOCK_PATTERN =
        Pattern.compile("(.*):  Replica placement policy is violated for (.*):(blk_\\d+)_(\\d+)\\. Block should be additionally replicated on 1 more rack\\(s\\)");

    private Configuration configuration;
    private UserGroupInformation ugi;
    private URLConnectionFactory connectionFactory;
    private boolean isSpnegoEnabled;
    private DistributedFileSystem fileSystem;
    private String checkPathName;
    private IEagleServiceClient serviceClient;
    private String siteId;
    private long firedTimestamp;

    @Override
    protected void prepare(JobExecutionContext context) throws JobExecutionException {
        Preconditions.checkArgument(context.getMergedJobDataMap().containsKey(HDFS_PROPS_KEY), HDFS_PROPS_KEY);
        Preconditions.checkArgument(context.getMergedJobDataMap().containsKey(SERVICE_HOST_KEY), SERVICE_HOST_KEY);
        Preconditions.checkArgument(context.getMergedJobDataMap().containsKey(SERVICE_PORT_KEY), SERVICE_PORT_KEY);

        try {
            // Previous dump under-replication blocks
            Properties properties = new Properties();
            String hdfsProps = context.getMergedJobDataMap().getString(HDFS_PROPS_KEY);
            if (StringUtils.isNotBlank(hdfsProps)) {
                properties.load(new StringReader(hdfsProps));
            }
            configuration = new Configuration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String propName = (String) entry.getKey();
                String propValue = (String) entry.getValue();
                configuration.set(propName, propValue);
            }
            this.ugi = UserGroupInformation.getCurrentUser();
            this.connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(configuration);
            this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
            this.fileSystem = (DistributedFileSystem) FileSystem.get(this.configuration);
            this.checkPathName = "/";
            this.serviceClient = new EagleServiceClientImpl(
                context.getMergedJobDataMap().getString(SERVICE_HOST_KEY),
                context.getMergedJobDataMap().getInt(SERVICE_PORT_KEY));
            this.siteId = "sandbox";
            this.firedTimestamp = context.getFireTime().getTime();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new JobExecutionException(e);
        }
    }

    @Override
    protected MonitorResult execute() throws Exception {
        // Compare current missing block and previous version of under-replicated
        int exitCode = UserGroupInformation.getCurrentUser().doAs(new PrivilegedAction<Integer>() {
            @Override
            public Integer run() {
                try {
                    return doCheck(checkPathName);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                return 1;
            }
        });

        if (exitCode == 0) {
            return MonitorResult.ok("HDFS is healthy");
        } else {
            return MonitorResult.critical("HDFS is not healthy", null);
        }
    }

    /**
     * As currently DFSClient don't support list under-replicated blocks, so still use HTTP to retrieve all under-replicated/rack-required/corrupt blocks.
     *
     * @throws IOException
     */
    private int listFsckReportUnderPath(Path path, String baseUrl) throws IOException {
        int errCode = 0;
        URL url = new URL(baseUrl + "&path=" + URLEncoder.encode(Path.getPathWithoutSchemeAndAuthority(path).toString(), "UTF-8"));
        LOGGER.info("Accessing {}", url);
        URLConnection connection;
        try {
            connection = connectionFactory.openConnection(url, isSpnegoEnabled);
        } catch (AuthenticationException e) {
            throw new IOException(e);
        }
        InputStream stream = connection.getInputStream();
        BufferedReader input = new BufferedReader(new InputStreamReader(
            stream, "UTF-8"));
        // String lastLine = null;
        Map<String, Set<LocatedBlock>> fileBlocks = new HashMap<>();
        HashSet<String> fileList = new HashSet<>();
        Map<String, Set<String>> fileUnderReplicatedBlockIds = new HashMap<>();
        Map<String, Set<String>> fileCorruptBlockIds = new HashMap<>();

        Function<String, LocatedBlocks> getCachedFileLocatedBlocks = new Function<String, LocatedBlocks>() {
            final Map<String, LocatedBlocks> fileLocatedBlocksCache = new HashMap<>();
            @Override
            public LocatedBlocks apply(String file) {
                if (fileLocatedBlocksCache.containsKey(file)) {
                    return fileLocatedBlocksCache.get(file);
                } else {
                    try {
                        LocatedBlocks blocks = fileSystem.getClient().getLocatedBlocks(file, 0);
                        this.fileLocatedBlocksCache.put(file, blocks);
                        return blocks;
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
                return null;
            }
        };


        try {
            String line;
            while ((line = input.readLine()) != null) {
                // lastLine = line;
                Matcher matcher = UNDER_REPLICATED_BLOCK_PATTERN.matcher(line);
                if (matcher.matches()) {
                    String file = matcher.group(1);
                    String blockPoolId = matcher.group(2);
                    String blockId = matcher.group(3);
                    // long generationTimestamp  = Long.valueOf(matcher.group(4));
                    // int blockReplicationFactor = Integer.valueOf(matcher.group(5));
                    // int blockReplicationNum = Integer.valueOf(matcher.group(6));
                    fileList.add(file);
                    if (!fileBlocks.containsKey(file)) {
                        fileBlocks.put(file, new HashSet<>());
                    }
                    fileBlocks.get(file).add(getLocatedBlock(getCachedFileLocatedBlocks.apply(file),blockPoolId, blockId));
                    if (!fileUnderReplicatedBlockIds.containsKey(file)) {
                        fileUnderReplicatedBlockIds.put(file, new HashSet<>());
                    }
                    fileUnderReplicatedBlockIds.get(file).add(blockId);
                    continue;
                }
                matcher = CORRUPT_BLOCK_PATTERN.matcher(line);
                if (matcher.matches()) {
                    String file = matcher.group(1);
                    String blockPoolId = matcher.group(2);
                    String blockId = matcher.group(3);

                    fileList.add(file);
                    if (!fileBlocks.containsKey(file)) {
                        fileBlocks.put(file, new HashSet<>());
                    }
                    fileBlocks.get(file).add(getLocatedBlock(getCachedFileLocatedBlocks.apply(file),blockPoolId, blockId));
                    if (!fileCorruptBlockIds.containsKey(file)) {
                        fileCorruptBlockIds.put(file, new HashSet<>());
                    }
                    fileCorruptBlockIds.get(file).add(blockId);
                    // continue;
                }
                // matcher = RACK_REPLICATED_BLOCK_PATTERN.matcher(line);
                // if (matcher.matches()) {
                //     String file = matcher.group(1);
                //     String blockPoolId = matcher.group(2);
                //     String blockId = matcher.group(3);
                //     // long generationTimestamp = Long.valueOf(matcher.group(4));
                //     int newRackRequired = Integer.valueOf(matcher.group(5));
                //     onRackReplicatedBlock(getCachedFileLocatedBlocks.apply(file),
                //         file, blockPoolId, blockId, newRackRequired);
                // }
            }
        } finally {
            input.close();
        }

        persistUnderReplicaAndCorruptDFSBlocks(this.firedTimestamp, fileList, fileBlocks,fileUnderReplicatedBlockIds, fileCorruptBlockIds);

        //    assert lastLine != null;
        //    if (lastLine.endsWith(NamenodeFsck.HEALTHY_STATUS)) {
        //        errCode = 0;
        //    } else if (lastLine.endsWith(NamenodeFsck.CORRUPT_STATUS) || lastLine.endsWith(NamenodeFsck.FAILURE_STATUS)) {
        //        errCode = 1;
        //    } else if (lastLine.endsWith(NamenodeFsck.NONEXISTENT_STATUS)) {
        //        errCode = 0;
        //    } else if (lastLine.contains("Incorrect blockId format:")) {
        //        errCode = 0;
        //    }
        //    return errCode;
        return 0;
    }

    private void persistUnderReplicaAndCorruptDFSBlocks(long version, Set<String> fileList, Map<String, Set<LocatedBlock>> fileBlocks, Map<String, Set<String>> fileUnderReplicatedBlockIds, Map<String, Set<String>> fileCorruptBlockIds) throws IOException {
        List<HDFSBlockEntity> blockEntities = new LinkedList<>();

        for (String file: fileList) {
            FileStatus fileStatus = this.fileSystem.getFileStatus(getResolvedPath(file));
            for (LocatedBlock locatedBlock: fileBlocks.get(file)) {
                if (fileUnderReplicatedBlockIds.containsKey(file) && fileUnderReplicatedBlockIds.get(file).contains(locatedBlock.getBlock().getBlockName())) {
                    HDFSBlockEntity entity = new HDFSBlockEntity(this.siteId, fileStatus, locatedBlock, true, version);
                    blockEntities.add(entity);
                } else { // if (fileCorruptBlockIds.containsKey(file) && fileCorruptBlockIds.get(file).contains(locatedBlock.getBlock().getBlockName())) {
                    HDFSBlockEntity entity = new HDFSBlockEntity(this.siteId, fileStatus, locatedBlock, false, version);
                    blockEntities.add(entity);
                }
            }
        }

        try {
            GenericServiceAPIResponseEntity<String> response = this.serviceClient.create(blockEntities);
            if (response.isSuccess()) {
                LOGGER.info("Succeed to persist {} block entities in version {}", response.getObj().size(), version);
                // Delete older version
                response = this.serviceClient.delete()
                    .byQuery(String.format("%s[@site=\"%s\" AND @modifiedTime < %s]{*}",HDFSBlockEntity.HDFS_BLOCK_SERVICE_NAME, this.siteId, version - SNAPSHOT_RENTENTION_MS))
                    .pageSize(Integer.MAX_VALUE)
                    .send();

                if (response.isSuccess()) {
                    LOGGER.info("Succeed to delete {} older block entities before version {}", response.getObj().size(), version);
                } else {
                    LOGGER.error("Failed to delete older block entities because: {}", response.getException());
                }
            } else {
                LOGGER.error("Failed to persist block entities for version {}: {}" ,version , response.getException());
            }
        } catch (EagleServiceClientException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private LocatedBlock getLocatedBlock(LocatedBlocks locatedBlocks, String poolId, String blockId) {
        for (LocatedBlock block: locatedBlocks.getLocatedBlocks()) {
            if (block.getBlock().getBlockPoolId().equals(poolId)
                && block.getBlock().getBlockName().equals(blockId)) {
                return block;
            }
        }
        throw new IllegalArgumentException("Unknown block " + poolId + ":" + blockId);
    }

    private int doCheck(String pathName) throws IOException {
        URI namenodeHttpAddress = null;
        Path checkPath = null;

        try {
            checkPath = getResolvedPath(pathName);
            namenodeHttpAddress = getCurrentNamenodeInfoAddress(checkPath);
        } catch (IOException ioe) {
            LOGGER.error("FileSystem is inaccessible due to:\n"
                + ioe.toString());
        }

        if (namenodeHttpAddress == null) {
            //Error message already output in {@link #getCurrentNamenodeInfoAddress()}
            LOGGER.error("DFSck exiting.");
            throw new IOException("namenodeHttpAddress  is null");
        }

        final StringBuilder url = new StringBuilder();
        url.append("/fsck?ugi=").append(ugi.getShortUserName());
        url.insert(0, namenodeHttpAddress.toString());
        LOGGER.info("Connecting to namenode via " + url.toString());
        listFsckReportUnderPath(checkPath, url.toString());
        return 0;
        //        URL path = new URL(url.toString());
        //        LOGGER.info("Accessing {}", path);
        //        URLConnection connection;
        //        try {
        //            connection = connectionFactory.openConnection(path, isSpnegoEnabled);
        //        } catch (AuthenticationException e) {
        //            throw new IOException(e);
        //        }
        //        InputStream stream = connection.getInputStream();
        //        BufferedReader input = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        //        String line = null;
        //        String lastLine = null;
        //        int errCode = -1;
        //        try {
        //            while ((line = input.readLine()) != null) {
        //                LOGGER.info(line);
        //                lastLine = line;
        //            }
        //        } finally {
        //            input.close();
        //        }
        //
        //        if (lastLine.endsWith(NamenodeFsck.HEALTHY_STATUS)) {
        //            errCode = 0;
        //        } else if (lastLine.endsWith(NamenodeFsck.CORRUPT_STATUS) || lastLine.endsWith(NamenodeFsck.FAILURE_STATUS)) {
        //            errCode = 1;
        //        } else if (lastLine.endsWith(NamenodeFsck.NONEXISTENT_STATUS)) {
        //            errCode = 0;
        //        } else if (lastLine.contains("Incorrect blockId format:")) {
        //            errCode = 0;
        //        }
        //        return errCode;
    }

    /**
     * Derive the namenode http address from the current file system,
     * either default or as set by "-fs" in the generic options.
     *
     * @return Returns http address or null if failure.
     * @throws IOException if we can't determine the active NN address
     */
    private URI getCurrentNamenodeInfoAddress(Path target) throws IOException {
        //String nnAddress = null;
        Configuration conf = this.configuration;

        //get the filesystem object to verify it is an HDFS system
        final FileSystem fs = target.getFileSystem(conf);
        if (!(fs instanceof DistributedFileSystem)) {
            LOGGER.error("FileSystem is " + fs.getUri());
            return null;
        }

        return DFSUtil.getInfoServer(HAUtil.getAddressOfActive(fs), conf,
            DFSUtil.getHttpClientScheme(conf));
    }

    private Path getResolvedPath(String dir) throws IOException {
        Path dirPath = new Path(dir);
        FileSystem fs = dirPath.getFileSystem(configuration);
        return fs.resolvePath(dirPath);
    }

    /*
    * To get the list, we need to call iteratively until the server says
    * there is no more left.
    * @return corrupt file blocks num
    */
    private Integer listCorruptFileBlocks(Path path, String baseUrl)
        throws IOException {
        int numCorrupt = 0;
        int cookie = 0;
        final String noCorruptLine = "has no CORRUPT files";
        final String noMoreCorruptLine = "has no more CORRUPT files";
        final String cookiePrefix = "Cookie:";
        boolean allDone = false;
        while (!allDone) {
            final StringBuffer urlBuilder = new StringBuffer(baseUrl);
            urlBuilder.append("&path=").append(URLEncoder.encode(Path.getPathWithoutSchemeAndAuthority(path).toString(), "UTF-8"));
            urlBuilder.append("&listcorruptfileblocks=1");
            if (cookie > 0) {
                urlBuilder.append("&startblockafter=").append(String.valueOf(cookie));
            }
            URL url = new URL(urlBuilder.toString());
            LOGGER.info("Accessing {}", url);
            URLConnection connection;
            try {
                connection = connectionFactory.openConnection(url, isSpnegoEnabled);
            } catch (AuthenticationException e) {
                throw new IOException(e);
            }
            InputStream stream = connection.getInputStream();
            BufferedReader input = new BufferedReader(new InputStreamReader(
                stream, "UTF-8"));
            try {
                String line = null;
                while ((line = input.readLine()) != null) {
                    if (DOT_ONLY_PATTERN.matcher(line).matches()) {
                        continue;
                    }
                    if (line.startsWith(cookiePrefix)) {
                        try {
                            cookie = Integer.parseInt(line.split("\t")[1]);
                        } catch (Exception e) {
                            allDone = true;
                            break;
                        }
                        continue;
                    }
                    if ((line.endsWith(noCorruptLine))
                        || (line.endsWith(noMoreCorruptLine))
                        || (line.endsWith(NamenodeFsck.NONEXISTENT_STATUS))) {
                        allDone = true;
                        break;
                    }
                    if ((line.isEmpty())
                        || (line.startsWith("FSCK started by"))
                        || (line.startsWith("The filesystem under path"))) {
                        continue;
                    }
                    numCorrupt++;
                    if (numCorrupt == 1) {
                        LOGGER.info("The list of corrupt files under path '"
                            + path + "' are:");
                    }
                    final String[] splits = line.split("\t");
                    if (splits.length > 1) {
                        onCorruptBlockFile(this.fileSystem.getClient().getLocatedBlocks(splits[0], 0), splits[1], null, splits[0]);
                    } else {
                        LOGGER.warn("Illegal corrupt file line: {}", line);
                    }
                }
            } finally {
                input.close();
            }
        }
        LOGGER.info("The filesystem under path '" + path + "' has "
            + numCorrupt + " CORRUPT files");
        return numCorrupt;
    }

    /**
     * Aggregate blockId by path then by host.
     */
    private void onCorruptBlockFile(LocatedBlocks locatedBlocks,String blockId, String blockPoolId, String path ) {
        for (LocatedBlock block: locatedBlocks.getLocatedBlocks()) {
            if (block.isCorrupt()) {
                LOGGER.info("Corrupt BLOCK: {}", block);
            }
        }
    }

    @Override
    protected void close() throws JobExecutionException {
        // Do nothing
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }

        if (this.serviceClient != null) {
            try {
                this.serviceClient.close();
            } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }
}