/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.spark.history.storm;

import org.apache.eagle.jpm.spark.history.config.SparkHistoryCrawlConfig;
import org.apache.eagle.jpm.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHDFS {

    private static final Logger LOG = LoggerFactory.getLogger(TestHDFS.class);
    public static void main(String[] args) throws Exception{
        SparkHistoryCrawlConfig config = new SparkHistoryCrawlConfig();

        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS", config.hdfsConfig.endpoint);
        conf.set("hdfs.kerberos.principal", config.hdfsConfig.principal);
        conf.set("hdfs.keytab.file", config.hdfsConfig.keytab);

        FileSystem hdfs = HDFSUtil.getFileSystem(conf);
        Path path = new Path("/logs/spark-events/local-1463002514438");
        boolean exists = hdfs.exists(path);
        LOG.info("File exists:{}", exists);

    }
}
