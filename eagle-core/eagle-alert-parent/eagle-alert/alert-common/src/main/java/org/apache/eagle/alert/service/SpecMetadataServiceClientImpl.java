/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.service;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SpecMetadataServiceClientImpl implements IMetadataServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(SpecMetadataServiceClientImpl.class);

    private static final String METADATA_SPOUT_PATH = "/specmetadata/spoutSpec";
    private static final String METADATA_ROUTER_PATH = "/specmetadata/routerSpec";
    private static final String METADATA_ALERTBOLT_PATH = "/specmetadata/alertBoltSpec";
    private static final String METADATA_PUBLISH_PATH = "/specmetadata/publishSpec";
    private static final String METADATA_SDS_PATH = "/specmetadata/sds";

    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";

    private String host;
    private int port;
    private String context;
    private transient Client client;
    private String basePath;

    public SpecMetadataServiceClientImpl(Config config) {
        this(config.getString(EAGLE_CORRELATION_SERVICE_HOST), config.getInt(EAGLE_CORRELATION_SERVICE_PORT), config
                .getString(EAGLE_CORRELATION_CONTEXT));
        basePath = buildBasePath();
    }

    public SpecMetadataServiceClientImpl(String host, int port, String context) {
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

    @Override
    public void addStreamingCluster(StreamingCluster cluster) {

    }

    @Override
    public void addStreamingClusters(List<StreamingCluster> clusters) {

    }

    @Override
    public List<StreamingCluster> listClusters() {
        return null;
    }

    @Override
    public List<Topology> listTopologies() {
        return null;
    }

    @Override
    public void addTopology(Topology t) {

    }

    @Override
    public void addTopologies(List<Topology> topologies) {

    }

    @Override
    public void addPolicy(PolicyDefinition policy) {

    }

    @Override
    public void addPolicies(List<PolicyDefinition> policies) {

    }

    @Override
    public List<PolicyDefinition> listPolicies() {
        return null;
    }

    @Override
    public void addStreamDefinition(StreamDefinition streamDef) {

    }

    @Override
    public void addStreamDefinitions(List<StreamDefinition> streamDefs) {

    }

    @Override
    public List<StreamDefinition> listStreams() {
        return null;
    }

    @Override
    public void addDataSource(Kafka2TupleMetadata k2t) {

    }

    @Override
    public void addDataSources(List<Kafka2TupleMetadata> k2ts) {

    }

    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        return null;
    }

    @Override
    public void addPublishment(Publishment pub) {

    }

    @Override
    public void addPublishments(List<Publishment> pubs) {

    }

    @Override
    public List<Publishment> listPublishment() {
        return null;
    }

    @Override
    public List<SpoutSpec> listSpoutMetadata() {
        return null;
    }

    @Override
    public ScheduleState getVersionedSpec() {
        return null;
    }

    @Override
    public ScheduleState getVersionedSpec(String version) {
        return null;
    }

    @Override
    public void addScheduleState(ScheduleState state) {

    }

    @Override
    public void clear() {

    }

    @Override
    public List<AlertPublishEvent> listAlertPublishEvent() {
        return null;
    }

    @Override
    public void addAlertPublishEvent(AlertPublishEvent event) {

    }

    @Override
    public void addAlertPublishEvents(List<AlertPublishEvent> events) {

    }

    @Override
    public void close() throws IOException {

    }

    public Map<String, StreamDefinition> getSds() {
        WebResource r = client.resource(basePath + METADATA_SDS_PATH);
        ClientResponse resp = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Map<String, StreamDefinition> sds = resp.getEntity(new GenericType<Map<String, StreamDefinition>>() {
        });
        return sds;
    }

    private <T> T listOne(String path, Class<T> tClz) {
        LOG.info("query URL {}", basePath + path);
        WebResource r = client.resource(basePath + path);

        ClientResponse resp = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        if (resp.getStatus() < 300) {
            try {
                return resp.getEntity(tClz);
            } catch (Exception e) {
                LOG.warn(" list one entity failed, ignored and continute, path {}, message {}!", path, e.getMessage());
            }
        } else {
            LOG.warn("fail querying metadata service {} with http status {}", basePath + path, resp.getStatus());
        }
        return null;
    }

    public SpoutSpec getSpoutSpec() {
        return listOne(METADATA_SPOUT_PATH, SpoutSpec.class);
    }

    public RouterSpec getRouterSpec() {
        return listOne(METADATA_ROUTER_PATH, RouterSpec.class);
    }

    public AlertBoltSpec getAlertBoltSpec() {
        return listOne(METADATA_ALERTBOLT_PATH, AlertBoltSpec.class);
    }

    public PublishSpec getPublishSpec() {
        return listOne(METADATA_PUBLISH_PATH, PublishSpec.class);
    }

}
