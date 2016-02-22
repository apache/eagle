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
package org.apache.eagle.correlation.client;

import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

/**
 * Created by yonzhang on 2/20/16.
 */
public class MetadataClientImpl implements IMetadataClient {
	protected static final String CONTENT_TYPE = "Content-Type";
	protected static final String DEFAULT_HTTP_HEADER_CONTENT_TYPE = "application/json";
	String basePath;

	Client client;

	public MetadataClientImpl(Config config) {
		String host = config.getString("eagle.correlation.serviceHost");
		int port = config.getInt("eagle.correlation.servicePort");
		this.basePath = "http://" + host + ":" + String.valueOf(port);
		ClientConfig cc = new DefaultClientConfig();
		cc.getProperties().put(DefaultClientConfig.PROPERTY_CONNECT_TIMEOUT,
				60 * 1000);
		cc.getProperties().put(DefaultClientConfig.PROPERTY_READ_TIMEOUT,
				60 * 1000);
		cc.getClasses().add(JacksonJsonProvider.class);
		cc.getProperties()
				.put(URLConnectionClientHandler.PROPERTY_HTTP_URL_CONNECTION_SET_METHOD_WORKAROUND,
						true);
		this.client = Client.create(cc);
	}

	@Override
	public List<String> findAllTopics() {
		WebResource r = client.resource(basePath + "/api/topics");
		return r.header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE).get(
				List.class);
	}

	@Override
	public Map<String, List<String>> findAllGroups() {
		WebResource r = client.resource(basePath + "/api/groups");
		return r.header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE).get(
				Map.class);
	}

	public static void main(String[] args) {
		Config config = ConfigFactory.load();
		MetadataClientImpl impl = new MetadataClientImpl(config);
		System.out.println(impl.findAllTopics());
	}
}
