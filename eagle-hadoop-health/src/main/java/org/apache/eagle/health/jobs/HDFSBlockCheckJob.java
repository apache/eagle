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
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.app.job.MonitorJob;
import org.apache.eagle.app.job.MonitorResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

@DisallowConcurrentExecution
public class HDFSBlockCheckJob extends MonitorJob {
    static {
        HdfsConfiguration.init();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBlockCheckJob.class);
    private static final String HDFS_PROPS_KEY = "connectionProps";
    private static final Pattern DOT_ONLY_PATTERN = Pattern.compile("^\\.+$");;

    private Configuration configuration;
    private UserGroupInformation ugi;
    private URLConnectionFactory connectionFactory;
    private boolean isSpnegoEnabled;
    private FileSystem fileSystem;

    @Override
    protected void prepare(JobExecutionContext context) throws JobExecutionException {
        Preconditions.checkArgument(context.getMergedJobDataMap().containsKey(HDFS_PROPS_KEY));
        try {
            // Previous dump under-replication blocks
            Properties properties = new Properties();
            String hdfsProps = context.getMergedJobDataMap().getString(HDFS_PROPS_KEY);
            if (StringUtils.isNotBlank(hdfsProps)) {
                properties.load(new StringReader(hdfsProps));
            }
            configuration = new Configuration();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String propName = (String) entry.getKey();
                String propValue = (String) entry.getValue();
                configuration.set(propName, propValue);
            }
            this.ugi = UserGroupInformation.getCurrentUser();
            this.connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(configuration);
            this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
            this.fileSystem = FileSystem.get(this.configuration);
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
                    return doWork();
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

    private void listUnderReplicateBlocks() {

    }

    private int doWork() throws IOException {
        final StringBuilder url = new StringBuilder();
        url.append("/fsck?ugi=").append(ugi.getShortUserName());
        String dir = "/";
        Path dirpath = null;
        URI namenodeAddress = null;
        try {
            dirpath = getResolvedPath(dir);
            namenodeAddress = getCurrentNamenodeAddress(dirpath);
        } catch (IOException ioe) {
            LOGGER.error("FileSystem is inaccessible due to:\n"
                    + ioe.toString());
        }

        if (namenodeAddress == null) {
            //Error message already output in {@link #getCurrentNamenodeAddress()}
            LOGGER.error("DFSck exiting.");
            return 0;
        }

        url.insert(0, namenodeAddress.toString());
        LOGGER.info("Connecting to namenode via " + url.toString());
        return listCorruptFileBlocks(dirpath, url.toString());

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
    private URI getCurrentNamenodeAddress(Path target) throws IOException {
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
        Map<String,Set<String>> fileCorruptBlockMap = new HashMap<>();

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
                        onCorruptBlock(splits[0], splits[1], baseUrl);
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
    private void onCorruptBlock(String blockId, String path, String baseUrl) {
        LOGGER.info("Corrupt block BlockId: {}, Path: {}", blockId, path);
        try {
            // =================================
            // Fsck report on corrupt file path
            // =================================
            URL url = new URL(baseUrl + "&path=" + path);
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
                    LOGGER.info(line);
                }
            } finally {
                input.close();
            }

            // =============================
            // Block location information
            // =============================
            RemoteIterator<LocatedFileStatus> statusIterator = this.fileSystem.listLocatedStatus(getResolvedPath(path));
            while (statusIterator.hasNext()) {
                LocatedFileStatus  status = statusIterator.next();
                BlockLocation[] blockLocations = status.getBlockLocations();
                for (BlockLocation blockLocation: blockLocations) {
                    if (blockLocation.isCorrupt()) {
                        LOGGER.info("Corrupt location (path: {}, user: {}, replica: {}): {}", path, status.getOwner(), status.getReplication(),blockLocation);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    protected void close() throws JobExecutionException {
        // Do nothing
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (IOException e) {
                throw new JobExecutionException(e);
            }
        }
    }
}