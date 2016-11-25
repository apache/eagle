/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.typesafe.config.Config;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SecurityDataEnrichServiceClientImpl implements ISecurityDataEnrichServiceClient {
    private static final long serialVersionUID = 3003976065082684128L;

    private static final Logger LOG = LoggerFactory.getLogger(SecurityDataEnrichServiceClientImpl.class);

    private static final String METADATA_LIST_HBASE_SENSITIVITY_PATH = "/metadata/security/hbaseSensitivity";
    private static final String METADATA_ADD_HBASE_SENSITIVITY_PATH = "/metadata/security/hbaseSensitivity";

    private static final String METADATA_LIST_HDFS_SENSITIVITY_PATH = "/metadata/security/hdfsSensitivity";
    private static final String METADATA_ADD_HDFS_SENSITIVITY_PATH = "/metadata/security/hdfsSensitivity";

    private static final String METADATA_LIST_OOZIE_SENSITIVITY_PATH = "/metadata/security/oozieSensitivity";
    private static final String METADATA_ADD_OOZIE_SENSITIVITY_PATH = "/metadata/security/oozieSensitivity";

    private static final String METADATA_LIST_HIVE_SENSITIVITY_PATH = "/metadata/security/hiveSensitivity";
    private static final String METADATA_ADD_HIVE_SENSITIVITY_PATH = "/metadata/security/hiveSensitivity";

    private static final String METADATA_LIST_IPZONE_PATH = "/metadata/security/ipzone";
    private static final String METADATA_ADD_IPZONE_PATH = "/metadata/security/ipzone";

    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";

    protected static final String CONTENT_TYPE = "Content-Type";

    private String host;
    private int port;
    private String context;
    private transient Client client;
    private String basePath;

    public SecurityDataEnrichServiceClientImpl(Config config) {
        this(config.getString(EAGLE_CORRELATION_SERVICE_HOST), config.getInt(EAGLE_CORRELATION_SERVICE_PORT), config
                .getString(EAGLE_CORRELATION_CONTEXT));
        basePath = buildBasePath();
    }

    public SecurityDataEnrichServiceClientImpl(String host, int port, String context) {
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

    private <T> List<T> list(String path, GenericType<List<T>> type) {
        WebResource r = client.resource(basePath + path);
        LOG.info("query URL {}", basePath + path);
        List<T> ret = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).get(type);
        return ret;
    }

    @Override
    public void close() throws IOException {
        client.destroy();
    }

    @Override
    public Collection<HBaseSensitivityEntity> listHBaseSensitivities() {
        return list(METADATA_LIST_HBASE_SENSITIVITY_PATH, new GenericType<List<HBaseSensitivityEntity>>() {
        });
    }

    @Override
    public OpResult addHBaseSensitivity(Collection<HBaseSensitivityEntity> h) {
        WebResource r = client.resource(basePath + METADATA_ADD_HBASE_SENSITIVITY_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(h);
        return new OpResult();
    }

    @Override
    public Collection<HdfsSensitivityEntity> listHdfsSensitivities() {
        return list(METADATA_LIST_HDFS_SENSITIVITY_PATH, new GenericType<List<HdfsSensitivityEntity>>() {
        });
    }

    @Override
    public OpResult addHdfsSensitivity(Collection<HdfsSensitivityEntity> h) {
        WebResource r = client.resource(basePath + METADATA_ADD_HDFS_SENSITIVITY_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(h);
        return new OpResult();
    }


    @Override
    public Collection<OozieSensitivityEntity> listOozieSensitivities() {
        return list(METADATA_LIST_OOZIE_SENSITIVITY_PATH, new GenericType<List<OozieSensitivityEntity>>() {
        });
    }

    @Override
    public OpResult addOozieSensitivity(Collection<OozieSensitivityEntity> h) {
        WebResource r = client.resource(basePath + METADATA_ADD_OOZIE_SENSITIVITY_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(h);
        return new OpResult();
    }

    @Override
    public Collection<IPZoneEntity> listIPZones(){
        return list(METADATA_LIST_IPZONE_PATH, new GenericType<List<IPZoneEntity>>() {
        });
    }

    @Override
    public OpResult addIPZone(Collection<IPZoneEntity> h){
        WebResource r = client.resource(basePath + METADATA_ADD_IPZONE_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(h);
        return new OpResult();
    }

    @Override
    public Collection<HiveSensitivityEntity> listHiveSensitivities() {
        return list(METADATA_LIST_HIVE_SENSITIVITY_PATH, new GenericType<List<HiveSensitivityEntity>>() {
        });
    }

    @Override
    public OpResult addHiveSensitivity(Collection<HiveSensitivityEntity> h) {
        WebResource r = client.resource(basePath + METADATA_ADD_HIVE_SENSITIVITY_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(h);
        return new OpResult();
    }
}
