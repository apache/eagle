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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class URLResourceFetcher {

    private static int MAX_RETRY_COUNT = 2;
    private static final Logger LOG = LoggerFactory.getLogger(URLResourceFetcher.class);

    public static InputStream openURLStream(String url) throws ServiceNotResponseException {
        return openURLStream(url, AppConstants.CompressionType.NONE);
    }

    public static InputStream openURLStream(String url, AppConstants.CompressionType compressionType) throws ServiceNotResponseException {
        InputStream is = null;
        LOG.info("Going to query URL {}", url);
        for (int i = 0; i < MAX_RETRY_COUNT; i++) {
            try {
                is = InputStreamUtils.getInputStream(url, null, compressionType);
            } catch (Exception e) {
                LOG.warn("fail to fetch data from {} due to {}, and try again", url, e.getMessage());
            }
        }
        if (is == null) {
            throw new ServiceNotResponseException(String.format("fail to fetch data from %s", url));
        } else {
            return is;
        }
    }

    public static void closeInputStream(InputStream is) {
        if (is != null) {
            try {
                is.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
