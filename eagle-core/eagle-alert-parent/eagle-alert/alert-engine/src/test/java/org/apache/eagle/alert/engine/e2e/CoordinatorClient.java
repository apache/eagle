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
package org.apache.eagle.alert.engine.e2e;

import java.io.Closeable;
import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.typesafe.config.Config;

/**
 * @since May 9, 2016
 *
 */
public class CoordinatorClient implements Closeable {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClient.class);

    private static final String EAGLE_COORDINATOR_SERVICE_CONTEXT = "coordinatorService.context";
    private static final String EAGLE_COORDINATOR_SERVICE_PORT = "coordinatorService.port";
    private static final String EAGLE_COORDINATOR_SERVICE_HOST = "coordinatorService.host";
    private static final String COORDINATOR_SCHEDULE_API = "/coordinator/build";

    private String host;
    private int port;
    private String context;
    private transient Client client;
    private String basePath;

    public CoordinatorClient(Config config) {
        this(config.getString(EAGLE_COORDINATOR_SERVICE_HOST), config.getInt(EAGLE_COORDINATOR_SERVICE_PORT), config
                .getString(EAGLE_COORDINATOR_SERVICE_CONTEXT));
        basePath = buildBasePath();
    }

    public CoordinatorClient(String host, int port, String context) {
        this.host = host;
        this.port = port;
        this.context = context;
        this.basePath = buildBasePath();
        ClientConfig cc = new DefaultClientConfig();
        cc.getProperties().put(DefaultClientConfig.PROPERTY_CONNECT_TIMEOUT, 60 * 1000);
        cc.getProperties().put(DefaultClientConfig.PROPERTY_READ_TIMEOUT, 60 * 1000);
        cc.getClasses().add(JacksonJsonProvider.class);
        cc.getProperties().put(URLConnectionClientHandler.PROPERTY_HTTP_URL_CONNECTION_SET_METHOD_WORKAROUND, true);
        this.client = Client.create(cc);
        client.addFilter(new com.sun.jersey.api.client.filter.GZIPContentEncodingFilter());
    }

    private String buildBasePath() {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append(context);
        return sb.toString();
    }

    public String schedule() {
        WebResource r = client.resource(basePath + COORDINATOR_SCHEDULE_API);
        return r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(String.class);
    }

    @Override
    public void close() throws IOException {
        this.client.destroy();
    }

}
