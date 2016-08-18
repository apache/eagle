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
package org.apache.eagle.common.service;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;

public class TrustAllSSLSocketFactory extends SSLSocketFactory
{
	private SSLSocketFactory socketFactory;
	public TrustAllSSLSocketFactory()
	{
		try {
			SSLContext ctx = SSLContext.getInstance("SSL");
//			ctx.init(null, new TrustManager[]{new TrustAnyTrustManager() {}}, new SecureRandom());
			ctx.init(null, new TrustManager[]{new TrustAnyTrustManager() {}}, null);
			socketFactory = ctx.getSocketFactory();
		} catch ( Exception ex ){ ex.printStackTrace(System.err);  /* handle exception */ }
	}
	public static SocketFactory getDefault(){
		return new TrustAllSSLSocketFactory();
	}
	@Override
	public String[] getDefaultCipherSuites()
	{
		return socketFactory.getDefaultCipherSuites();
	}
	@Override
	public String[] getSupportedCipherSuites()
	{
		return socketFactory.getSupportedCipherSuites();
	}
	@Override
	public Socket createSocket(Socket socket, String string, int i, boolean bln) throws IOException
	{
		return socketFactory.createSocket(socket, string, i, bln);
	}
	@Override
	public Socket createSocket(String string, int i) throws IOException, UnknownHostException
	{
		return socketFactory.createSocket(string, i);
	}
	@Override
	public Socket createSocket(String string, int i, InetAddress ia, int i1) throws IOException, UnknownHostException
	{
		return socketFactory.createSocket(string, i, ia, i1);
	}
	@Override
	public Socket createSocket(InetAddress ia, int i) throws IOException
	{
		return socketFactory.createSocket(ia, i);
	}
	@Override
	public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1) throws IOException
	{
		return socketFactory.createSocket(ia, i, ia1, i1);
	}

	private static class TrustAnyTrustManager implements X509TrustManager {
		@Override
		public void checkClientTrusted( final X509Certificate[] chain, final String authType ) {
		}
		@Override
		public void checkServerTrusted( final X509Certificate[] chain, final String authType ) {
		}
		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}
	}
}

