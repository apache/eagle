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
package org.apache.eagle.jobrunning.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.eagle.jobrunning.common.JobConstants;

public class InputStreamUtils {

	private static final int CONNECTION_TIMEOUT = 10 * 1000;
	private static final int READ_TIMEOUT = 5 * 60 * 1000;
	private static final String GZIP_HTTP_HEADER = "Accept-Encoding";
	private static final String GZIP_COMPRESSION = "gzip";
	
	private static InputStream openGZIPInputStream(URL url, int timeout) throws IOException {
		final URLConnection connection = url.openConnection();
		connection.setConnectTimeout(CONNECTION_TIMEOUT);
		connection.setReadTimeout(timeout);
		connection.addRequestProperty(GZIP_HTTP_HEADER, GZIP_COMPRESSION);
		return new GZIPInputStream(connection.getInputStream());
	}
	
	private static InputStream openInputStream(URL url, int timeout) throws IOException {
		URLConnection connection = url.openConnection();
		connection.setConnectTimeout(timeout);
		return connection.getInputStream();
	}
	
	public static InputStream getInputStream(String urlString, JobConstants.CompressionType compressionType, int timeout) throws Exception {
		final URL url = URLConnectionUtils.getUrl(urlString);
		if (compressionType.equals(JobConstants.CompressionType.GZIP)) {
			return openGZIPInputStream(url, timeout);
		}
		else { // CompressionType.NONE
			return openInputStream(url, timeout);
		}
	}
	
	public static InputStream getInputStream(String urlString, JobConstants.CompressionType compressionType) throws Exception {
		return getInputStream(urlString, compressionType, READ_TIMEOUT);
	}
}
