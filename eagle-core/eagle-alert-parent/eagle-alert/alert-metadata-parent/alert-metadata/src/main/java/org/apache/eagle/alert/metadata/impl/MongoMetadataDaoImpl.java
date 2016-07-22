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
package org.apache.eagle.alert.metadata.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.MetadataUtils;
import org.apache.eagle.alert.metadata.resource.Models;
import org.apache.eagle.alert.metadata.resource.OpResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Function;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.typesafe.config.Config;

/**
 * @since Apr 11, 2016
 *
 */
public class MongoMetadataDaoImpl implements IMetadataDao {

    private static final String DB_NAME = "ump_alert_metadata";
    private static final Logger LOG = LoggerFactory.getLogger(MongoMetadataDaoImpl.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final String connection;
    private final MongoClient client;

    private MongoDatabase db;
    private MongoCollection<Document> cluster;
    private MongoCollection<Document> schema;
    private MongoCollection<Document> datasource;
    private MongoCollection<Document> policy;
    private MongoCollection<Document> publishment;
    private MongoCollection<Document> publishmentType;
    private MongoCollection<Document> scheduleStates;
    private MongoCollection<Document> assignments;
    private MongoCollection<Document> topologies;

    public MongoMetadataDaoImpl(Config config) {
        this.connection = config.getString("connection");
        this.client = new MongoClient(new MongoClientURI(this.connection));
        init();
    }

    private void init() {
        db = client.getDatabase(DB_NAME);
        IndexOptions io = new IndexOptions().background(true).unique(true).name("nameIndex");
        BsonDocument doc = new BsonDocument();
        doc.append("name", new BsonInt32(1));
        cluster = db.getCollection("clusters");
        cluster.createIndex(doc, io);
        {
            BsonDocument doc2 = new BsonDocument();
            doc2.append("streamId", new BsonInt32(1));
            schema = db.getCollection("schemas");
            schema.createIndex(doc2, io);
        }
        datasource = db.getCollection("datasources");
        datasource.createIndex(doc, io);
        policy = db.getCollection("policies");
        policy.createIndex(doc, io);
        publishment = db.getCollection("publishments");
        publishment.createIndex(doc, io);
        topologies = db.getCollection("topologies");
        topologies.createIndex(doc, io);

        publishmentType = db.getCollection("publishmentTypes");
        {
            IndexOptions io1 = new IndexOptions().background(true).unique(true).name("pubTypeIndex");
            BsonDocument doc1 = new BsonDocument();
            doc1.append("type", new BsonInt32(1));
            publishmentType.createIndex(doc1, io1);
        }

        scheduleStates = db.getCollection("schedule_specs");
        {
            IndexOptions io1 = new IndexOptions().background(true).unique(true).name("nameIndex");
            BsonDocument doc1 = new BsonDocument();
            doc1.append("version", new BsonInt32(1));
            scheduleStates.createIndex(doc1, io1);
        }

        assignments = db.getCollection("assignments");
        {
            IndexOptions io1 = new IndexOptions().background(true).unique(true).name("policyNameIndex");
            BsonDocument doc1 = new BsonDocument();
            doc1.append("policyName", new BsonInt32(1));
            assignments.createIndex(doc1, io1);
        }
    }

    @Override
    public List<StreamingCluster> listClusters() {
        return list(cluster, StreamingCluster.class);
    }

    private <T> List<T> list(MongoCollection<Document> collection, Class<T> clz) {
        List<T> result = new LinkedList<T>();
        collection.find().map(new Function<Document, T>() {
            @Override
            public T apply(Document t) {
                String json = t.toJson();
                try {
                    return mapper.readValue(json, clz);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
                return null;
            }
        }).into(result);
        return result;
    }

    private <T> OpResult addOrReplace(MongoCollection<Document> collection, T t) {
        BsonDocument filter = new BsonDocument();
        if (t instanceof StreamDefinition) {
            filter.append("streamId", new BsonString(MetadataUtils.getKey(t)));
        } else if (t instanceof PublishmentType) {
            filter.append("type", new BsonString(MetadataUtils.getKey(t)));
        } else {
            filter.append("name", new BsonString(MetadataUtils.getKey(t)));
        }

        String json = "";
        OpResult result = new OpResult();
        try {
            json = mapper.writeValueAsString(t);
            UpdateOptions options = new UpdateOptions();
            options.upsert(true);
            UpdateResult ur = collection.replaceOne(filter, Document.parse(json), options);
            // FIXME: could based on matched count do better matching...
            if (ur.getModifiedCount() > 0 || ur.getUpsertedId() != null) {
                result.code = 200;
                result.message = String.format("update %d configuration item.", ur.getModifiedCount());
            } else {
                result.code = 500;
                result.message = "no configuration item create/updated.";
            }
        } catch (Exception e) {
            result.code = 500;
            result.message = e.getMessage();
            LOG.error("", e);
        }
        return result;
    }

    private <T> OpResult remove(MongoCollection<Document> collection, String name) {
        BsonDocument filter = new BsonDocument();
        filter.append("name", new BsonString(name));
        DeleteResult dr = collection.deleteOne(filter);
        OpResult result = new OpResult();
        result.code = 200;
        result.message = String.format(" %d config item removed!", dr.getDeletedCount());
        return result;
    }

    @Override
    public OpResult addCluster(StreamingCluster cluster) {
        return addOrReplace(this.cluster, cluster);
    }

    @Override
    public OpResult removeCluster(String clusterId) {
        return remove(cluster, clusterId);
    }

    @Override
    public List<StreamDefinition> listStreams() {
        return list(schema, StreamDefinition.class);
    }

    @Override
    public OpResult createStream(StreamDefinition stream) {
        return addOrReplace(this.schema, stream);
    }

    @Override
    public OpResult removeStream(String streamId) {
        return remove(schema, streamId);
    }

    @Override
    public List<Kafka2TupleMetadata> listDataSources() {
        return list(datasource, Kafka2TupleMetadata.class);
    }

    @Override
    public OpResult addDataSource(Kafka2TupleMetadata dataSource) {
        return addOrReplace(this.datasource, dataSource);
    }

    @Override
    public OpResult removeDataSource(String datasourceId) {
        return remove(datasource, datasourceId);
    }

    @Override
    public List<PolicyDefinition> listPolicies() {
        return list(policy, PolicyDefinition.class);
    }

    @Override
    public OpResult addPolicy(PolicyDefinition policy) {
        return addOrReplace(this.policy, policy);
    }

    @Override
    public OpResult removePolicy(String policyId) {
        return remove(policy, policyId);
    }

    @Override
    public List<Publishment> listPublishment() {
        return list(publishment, Publishment.class);
    }

    @Override
    public OpResult addPublishment(Publishment publishment) {
        return addOrReplace(this.publishment, publishment);
    }

    @Override
    public OpResult removePublishment(String pubId) {
        return remove(publishment, pubId);
    }

    @Override
    public List<PublishmentType> listPublishmentType() {
        return list(publishmentType, PublishmentType.class);
    }

    @Override
    public OpResult addPublishmentType(PublishmentType pubType) {
        return addOrReplace(this.publishmentType, pubType);
    }

    @Override
    public OpResult removePublishmentType(String pubType) {
        return remove(publishmentType, pubType);
    }

    @Override
    public ScheduleState getScheduleState(String versionId) {
        BsonDocument doc = new BsonDocument();
        doc.append("version", new BsonString(versionId));
        ScheduleState state = scheduleStates.find(doc).map(new Function<Document, ScheduleState>() {
            @Override
            public ScheduleState apply(Document t) {
                String json = t.toJson();
                try {
                    return mapper.readValue(json, ScheduleState.class);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
                return null;
            }
        }).first();
        return state;
    }

    @Override
    public OpResult addScheduleState(ScheduleState state) {
        return addOne(scheduleStates, state);
    }

    private <T> OpResult addOne(MongoCollection<Document> collection, T t) {
        OpResult result = new OpResult();
        try {
            String json = mapper.writeValueAsString(t);
            collection.insertOne(Document.parse(json));
            result.code = 200;
            result.message = "add state succeed!";
        } catch (Exception e) {
            result.code = 400;
            result.message = e.getMessage();
            LOG.error("", e);
        }
        return result;
    }

    @Override
    public ScheduleState getScheduleState() {
        BsonDocument sort = new BsonDocument();
        sort.append("generateTime", new BsonInt32(-1));
        ScheduleState state = scheduleStates.find().sort(sort).map(new Function<Document, ScheduleState>() {
            @Override
            public ScheduleState apply(Document t) {
                String json = t.toJson();
                try {
                    return mapper.readValue(json, ScheduleState.class);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
                return null;
            }
        }).first();
        return state;
    }

    @Override
    public List<PolicyAssignment> listAssignments() {
        return list(assignments, PolicyAssignment.class);
    }

    @Override
    public OpResult addAssignment(PolicyAssignment assignment) {
        return addOne(assignments, assignment);
    }

    @Override
    public List<Topology> listTopologies() {
        return list(topologies, Topology.class);
    }

    @Override
    public OpResult addTopology(Topology t) {
        return addOrReplace(this.topologies, t);
    }

    @Override
    public OpResult removeTopology(String topologyName) {
        return remove(topologies, topologyName);
    }

    @Override
    public OpResult clear() {
        throw new UnsupportedOperationException("clear not support!");
    }

    @Override
    public Models export() {
        throw new UnsupportedOperationException("export not support!");
    }

    @Override
    public OpResult importModels(Models models) {
        throw new UnsupportedOperationException("importModels not support!");
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
