/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.typesafe.config.Config;

public class MetadataServiceClientImpl implements IMetadataServiceClient {
    private static final long serialVersionUID = 3003976065082684128L;

    private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceClientImpl.class);

    private static final String METADATA_SCHEDULESTATES_PATH = "/metadata/schedulestates";
    private static final String METADATA_PUBLISHMENTS_PATH = "/metadata/publishments";
    private static final String METADATA_DATASOURCES_PATH = "/metadata/datasources";
    private static final String METADATA_STREAMS_PATH = "/metadata/streams";
    private static final String METADATA_POLICIES_PATH = "/metadata/policies";
    private static final String METADATA_CLUSTERS_PATH = "/metadata/clusters";
    private static final String METADATA_TOPOLOGY_PATH = "/metadata/topologies";

    private static final String METADATA_PUBLISHMENTS_BATCH_PATH = "/metadata/publishments/batch";
    private static final String METADATA_DATASOURCES_BATCH_PATH = "/metadata/datasources/batch";
    private static final String METADATA_STREAMS_BATCH_PATH = "/metadata/streams/batch";
    private static final String METADATA_POLICIES_BATCH_PATH = "/metadata/policies/batch";
    private static final String METADATA_CLUSTERS_BATCH_PATH = "/metadata/clusters/batch";
    private static final String METADATA_TOPOLOGY_BATCH_PATH = "/metadata/topologies/batch";

    private static final String METADATA_CLEAR_PATH = "/metadata/clear";

    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";

    protected static final String CONTENT_TYPE = "Content-Type";

    private String host;
    private int port;
    private String context;
    private transient Client client;
    private String basePath;

    public MetadataServiceClientImpl(Config config) {
        this(config.getString(EAGLE_CORRELATION_SERVICE_HOST), config.getInt(EAGLE_CORRELATION_SERVICE_PORT), config
                .getString(EAGLE_CORRELATION_CONTEXT));
        basePath = buildBasePath();
    }

    public MetadataServiceClientImpl(String host, int port, String context) {
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
        if (host.startsWith("http://") || host.startsWith("https://")) {
            return host + ":" + port + context;
        } else {
            // https
            if (port == 443) {
                return "https://" + host + ":" + port + context;
            } else {
                // http
                return "http://" + host + ":" + port + context;
            }
        }
    }

    @Override
    public void close() throws IOException {
        client.destroy();
    }

    @Override
    public List<SpoutSpec> listSpoutMetadata() {
        ScheduleState state = getVersionedSpec();
        return new ArrayList<>(state.getSpoutSpecs().values());
    }

    @Override
    public List<StreamingCluster> listClusters() {
        return list(METADATA_CLUSTERS_PATH, new GenericType<List<StreamingCluster>>() {
        });
    }

    @Override
    public List<PolicyDefinition> listPolicies() {
        return list(METADATA_POLICIES_PATH, new GenericType<List<PolicyDefinition>>() {
        });
    }

    @Override
    public List<StreamDefinition> listStreams() {
        return list(METADATA_STREAMS_PATH, new GenericType<List<StreamDefinition>>() {
        });
    }

    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        return list(METADATA_DATASOURCES_PATH, new GenericType<List<Kafka2TupleMetadata>>() {
        });
    }

    private <T> List<T> list(String path, GenericType<List<T>> type) {
        WebResource r = client.resource(basePath + path);
        LOG.info("query URL {}", basePath + path);
        List<T> ret = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).get(type);
        return ret;
    }

    @Override
    public List<Publishment> listPublishment() {
        return list(METADATA_PUBLISHMENTS_PATH, new GenericType<List<Publishment>>() {
        });
    }

    @Override
    public ScheduleState getVersionedSpec(String version) {
        return listOne(METADATA_SCHEDULESTATES_PATH + "/" + version, ScheduleState.class);
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
        }else{
            LOG.warn("fail querying metadata service {} with http status {}", basePath + path, resp.getStatus());
        }
        return null;
    }

    @Override
    public ScheduleState getVersionedSpec() {
        return listOne(METADATA_SCHEDULESTATES_PATH, ScheduleState.class);
    }

    @Override
    public void addScheduleState(ScheduleState state) {
        WebResource r = client.resource(basePath + METADATA_SCHEDULESTATES_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(state);
    }

    @Override
    public List<Topology> listTopologies() {
        return list(METADATA_TOPOLOGY_PATH, new GenericType<List<Topology>>() {
        });
    }

    @Override
    public void addStreamingCluster(StreamingCluster cluster) {
        WebResource r = client.resource(basePath + METADATA_CLUSTERS_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(cluster);
    }

    @Override
    public void addStreamingClusters(List<StreamingCluster> clusters) {
        WebResource r = client.resource(basePath + METADATA_CLUSTERS_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(clusters);
    }

    @Override
    public void addTopology(Topology t) {
        WebResource r = client.resource(basePath + METADATA_TOPOLOGY_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(t);
    }

    @Override
    public void addTopologies(List<Topology> topologies) {
        WebResource r = client.resource(basePath + METADATA_TOPOLOGY_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(topologies);
    }

    @Override
    public void addPolicy(PolicyDefinition policy) {
        WebResource r = client.resource(basePath + METADATA_POLICIES_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(policy);
    }

    @Override
    public void addPolicies(List<PolicyDefinition> policies) {
        WebResource r = client.resource(basePath + METADATA_POLICIES_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(policies);
    }

    @Override
    public void addStreamDefinition(StreamDefinition streamDef) {
        WebResource r = client.resource(basePath + METADATA_STREAMS_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(streamDef);
    }

    @Override
    public void addStreamDefinitions(List<StreamDefinition> streamDefs) {
        WebResource r = client.resource(basePath + METADATA_STREAMS_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(streamDefs);
    }

    @Override
    public void addDataSource(Kafka2TupleMetadata k2t) {
        WebResource r = client.resource(basePath + METADATA_DATASOURCES_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(k2t);
    }

    @Override
    public void addDataSources(List<Kafka2TupleMetadata> k2ts) {
        WebResource r = client.resource(basePath + METADATA_DATASOURCES_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(k2ts);
    }

    @Override
    public void addPublishment(Publishment pub) {
        WebResource r = client.resource(basePath + METADATA_PUBLISHMENTS_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(pub);
    }

    @Override
    public void addPublishments(List<Publishment> pubs) {
        WebResource r = client.resource(basePath + METADATA_PUBLISHMENTS_BATCH_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post(pubs);
    }

    @Override
    public void clear() {
        WebResource r = client.resource(basePath + METADATA_CLEAR_PATH);
        r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON).post();
    }

}
