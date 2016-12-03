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
