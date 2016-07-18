package org.apache.eagle.jpm.util.resourceFetch.url;

/**
 * URL utils.
 */
public class URLUtil {
    public static String removeTrailingSlash(String url) {
        int i = url.length() - 1;
        // Skip all slashes from the end.
        while (i >= 0 && url.charAt(i) == '/') {
            i--;
        }

        return url.substring(0, i + 1);
    }
}
