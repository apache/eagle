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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class URLConnectionUtils {
	//TODO: change some public method to private
    private static final Logger LOG = LoggerFactory.getLogger(URLConnectionUtils.class);
	
	public static URLConnection getConnection(String url) throws Exception {
		if (url.startsWith("https://")) {
			return getHTTPSConnection(url);
		} else if (url.startsWith("http://")) {
			return getHTTPConnection(url);
		}
		throw new Exception("Invalid input argument url: " + url);
	}

	public static URLConnection getHTTPConnection(String urlString) throws Exception {
		final URL url = new URL(urlString);
		return url.openConnection();
	}

	public static URL getUrl(String urlString) throws Exception  {
		if(urlString.toLowerCase().contains("https")){
			return getHTTPSUrl(urlString);
		}else if (urlString.toLowerCase().contains("http")) {
			return getURL(urlString);
		}
		throw new Exception("Invalid input argument url: " + urlString);
	}
	
	public static URL getURL(String urlString) throws MalformedURLException {
		return new URL(urlString);
	}
	
	public static URL getHTTPSUrl(String urlString) throws MalformedURLException, NoSuchAlgorithmException, KeyManagementException  {
    	// Create a trust manager that does not validate certificate chains   
        final TrustManager[] trustAllCerts = new TrustManager[] {new TrustAllX509TrustManager()};
        // Install the all-trusting trust manager   
        final SSLContext sc = SSLContext.getInstance("SSL");   
        sc.init(null, trustAllCerts, new java.security.SecureRandom());   
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());   
        // Create all-trusting host name verifier   
        final HostnameVerifier allHostsValid = new HostnameVerifier() {   
            public boolean verify(String hostname, SSLSession session) {   
                return true;   
            }   
        };
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        return new URL(urlString);
	}

	public static URLConnection getHTTPSConnection(String urlString) throws IOException, KeyManagementException, NoSuchAlgorithmException  {
       	final URL url = getHTTPSUrl(urlString);
       	return url.openConnection();
	}
	
	public static class TrustAllX509TrustManager implements X509TrustManager {
		@Override
		public void checkClientTrusted(
				java.security.cert.X509Certificate[] chain, String authType)
				throws CertificateException {
		}

		@Override
		public void checkServerTrusted(
				java.security.cert.X509Certificate[] chain, String authType)
				throws CertificateException {
		}

		@Override
		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return null;
		}
	}
}
