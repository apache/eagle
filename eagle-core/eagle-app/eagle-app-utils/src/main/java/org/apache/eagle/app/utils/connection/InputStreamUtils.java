/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.eagle.app.utils.connection;

import org.apache.eagle.app.utils.AppConstants;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

public class InputStreamUtils {

    private static final int CONNECTION_TIMEOUT = 1 * 30 * 1000;
    private static final int READ_TIMEOUT = 1 * 60 * 1000;
    private static final String GZIP_HTTP_HEADER = "Accept-Encoding";
    private static final String GZIP_COMPRESSION = "gzip";

    private static InputStream openGZIPInputStream(URL url, String auth, int timeout) throws IOException {
        final URLConnection connection = url.openConnection();
        connection.setConnectTimeout(CONNECTION_TIMEOUT);
        connection.setReadTimeout(timeout);
        connection.addRequestProperty(GZIP_HTTP_HEADER, GZIP_COMPRESSION);
        if (null != auth) {
            connection.setRequestProperty("Authorization", auth);
        }
        return new GZIPInputStream(connection.getInputStream());
    }

    private static InputStream openInputStream(URL url, String auth, int timeout) throws IOException {
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(timeout);
        if (null != auth) {
            connection.setRequestProperty("Authorization", auth);
        }
        return connection.getInputStream();
    }

    public static InputStream getInputStream(String urlString, String auth, AppConstants.CompressionType compressionType, int timeout) throws Exception {
        final URL url = URLConnectionUtils.getUrl(urlString);
        if (compressionType.equals(AppConstants.CompressionType.GZIP)) {
            return openGZIPInputStream(url, auth, timeout);
        } else { // CompressionType.NONE
            return openInputStream(url, auth, timeout);
        }
    }

    public static InputStream getInputStream(String urlString, String auth, AppConstants.CompressionType compressionType) throws Exception {
        return getInputStream(urlString, auth, compressionType, READ_TIMEOUT);
    }

}
