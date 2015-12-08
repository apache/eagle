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
package org.apache.eagle.service.security.hdfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.eagle.security.util.HadoopSecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class is responsible for all FileSystem Querying Operation
 * Example 
 * List of Directories
 * List of Files and It's Paths
 * 
 * This internally it uses Hadoop FileSystem API to List the files
 * 
 */
public class HDFSFileSystem {

	private Configuration config;

	private static Logger LOG = LoggerFactory.getLogger(HDFSFileSystem.class);
	
	public HDFSFileSystem(Configuration hdfsConfig) {
		config = hdfsConfig;
	}

	public FileSystem getFileSystem() throws IOException {
		HadoopSecurityUtil.login(config);
		return FileSystem.get(config);
	}
	
	/**
	 * Brows the Files for the specific Path
     *
	 * @param filePath
	 * @return listOfFiles
	 * @throws Exception 
	 */
	public List<FileStatus> browse(String filePath) throws Exception {
		FileSystem hdfsFileSystem = null;
        FileStatus[]  listStatus;
        try {
			hdfsFileSystem = getFileSystem();
			Path path  = new Path(filePath);
			listStatus = hdfsFileSystem.listStatus( path );
		} catch ( Exception ex ) {
			LOG.error(" Exception when browsing files for the path " +filePath , ex.getMessage() );
			throw new Exception(" Exception When browsing Files in HDFS .. Message :  "+ex.getMessage());
		} finally {
			 //Close the file system
			if( hdfsFileSystem != null ) hdfsFileSystem.close();
		}
		return Arrays.asList(listStatus);
	}

}