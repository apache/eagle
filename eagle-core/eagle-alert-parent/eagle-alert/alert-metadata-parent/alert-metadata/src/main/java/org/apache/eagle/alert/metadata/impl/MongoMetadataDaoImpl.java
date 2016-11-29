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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.coordination.model.internal.MonitoredStream;
import org.apache.eagle.alert.coordination.model.internal.PolicyAssignment;
import org.apache.eagle.alert.coordination.model.internal.ScheduleStateBase;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @since Apr 11, 2016.
 */
public class MongoMetadataDaoImpl implements IMetadataDao {

    private static final String DB_NAME = "ump_alert_metadata";
    private static final Logger LOG = LoggerFactory.getLogger(MongoMetadataDaoImpl.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int DEFAULT_CAPPED_MAX_SIZE = 500 * 1024 * 1024;
    private static final int DEFAULT_CAPPED_MAX_DOCUMENTS = 20000;
    private static final String MANGO_CAPPED_MAX_SIZE = "mongo.cappedMaxSize";
    private static final String MANGO_CAPPED_MAX_DOCUMENTS = "mongo.cappedMaxDocuments";

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final String connection;
    private final MongoClient client;
    private final int cappedMaxSize;
    private final int cappedMaxDocuments;

    private MongoDatabase db;
    private MongoCollection<Document> cluster;
    private MongoCollection<Document> schema;
    private MongoCollection<Document> datasource;
    private MongoCollection<Document> policy;
    private MongoCollection<Document> publishment;
    private MongoCollection<Document> publishmentType;
    private MongoCollection<Document> topologies;
    private MongoCollection<Document> alerts;

    // scheduleStates splits to several collections
    private MongoCollection<Document> scheduleStates;
    private MongoCollection<Document> spoutSpecs;
    private MongoCollection<Document> alertSpecs;
    private MongoCollection<Document> groupSpecs;
    private MongoCollection<Document> publishSpecs;
    private MongoCollection<Document> policySnapshots;
    private MongoCollection<Document> streamSnapshots;
    private MongoCollection<Document> monitoredStreams;
    private MongoCollection<Document> assignments;

    @Inject
    public MongoMetadataDaoImpl(Config config) {
        this.connection = config.getString(MetadataUtils.MONGO_CONNECTION_PATH);
        this.cappedMaxSize = config.hasPath(MANGO_CAPPED_MAX_SIZE) ? config.getInt(MANGO_CAPPED_MAX_SIZE) : DEFAULT_CAPPED_MAX_SIZE;
        this.cappedMaxDocuments = config.hasPath(MANGO_CAPPED_MAX_DOCUMENTS) ? config.getInt(MANGO_CAPPED_MAX_DOCUMENTS) : DEFAULT_CAPPED_MAX_DOCUMENTS;
        this.client = new MongoClient(new MongoClientURI(this.connection));
        init();
    }

    private boolean isCollectionExists(String collectionName) {
        boolean result = false;
        MongoIterable<String> allCollections = db.listCollectionNames();
        for ( String collection : allCollections ) {
            if (collection.equals(collectionName)) {
                result = true;
                break;
            }
        }

        return result;
    }

    private MongoCollection<Document> getCollection(String collectionName) {
        // first check if collection exists, if not then create a new collection with cappedSize
        if (!isCollectionExists(collectionName)) {
            CreateCollectionOptions option = new CreateCollectionOptions();
            option.capped(true);
            option.maxDocuments(cappedMaxDocuments);
            option.sizeInBytes(cappedMaxSize);
            db.createCollection(collectionName, option);
        }

        return db.getCollection(collectionName);

    }

    private void init() {
        db = client.getDatabase(DB_NAME);
        IndexOptions io = new IndexOptions().background(true).name("nameIndex");
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

        alerts = db.getCollection("alerts");
        {
            IndexOptions io1 = new IndexOptions().background(true).unique(true).name("alertIndex");
            BsonDocument doc1 = new BsonDocument();
            doc1.append("alertId", new BsonInt32(1));
            alerts.createIndex(doc1, io1);
        }


        // below is for schedule_specs and its splitted collections
        BsonDocument doc1 = new BsonDocument();
        IndexOptions io1 = new IndexOptions().background(true).name("versionIndex");
        doc1.append("version", new BsonInt32(1));
        scheduleStates = getCollection("schedule_specs");
        scheduleStates.createIndex(doc1, io1);

        spoutSpecs = getCollection("spoutSpecs");
        {
            IndexOptions ioInternal = new IndexOptions().background(true).name("topologyIdIndex");
            BsonDocument docInternal = new BsonDocument();
            docInternal.append("topologyId", new BsonInt32(1));
            spoutSpecs.createIndex(docInternal, ioInternal);
        }

        alertSpecs = getCollection("alertSpecs");
        {
            IndexOptions ioInternal = new IndexOptions().background(true).name("topologyNameIndex");
            BsonDocument docInternal = new BsonDocument();
            docInternal.append("topologyName", new BsonInt32(1));
            alertSpecs.createIndex(docInternal, ioInternal);
        }

        groupSpecs = getCollection("groupSpecs");
        groupSpecs.createIndex(doc1, io1);

        publishSpecs = getCollection("publishSpecs");
        publishSpecs.createIndex(doc1, io1);

        policySnapshots = getCollection("policySnapshots");
        policySnapshots.createIndex(doc1, io);

        streamSnapshots = getCollection("streamSnapshots");
        streamSnapshots.createIndex(doc1, io);

        monitoredStreams = getCollection("monitoredStreams");
        monitoredStreams.createIndex(doc1, io);

        assignments = getCollection("assignments");
        assignments.createIndex(doc1, io1);
    }

    @Override
    public List<StreamingCluster> listClusters() {
        return list(cluster, StreamingCluster.class);
    }


    private <T> OpResult addOrReplace(MongoCollection<Document> collection, T t) {
        BsonDocument filter = new BsonDocument();
        if (t instanceof StreamDefinition) {
            filter.append("streamId", new BsonString(MetadataUtils.getKey(t)));
        } else if (t instanceof PublishmentType) {
            filter.append("type", new BsonString(MetadataUtils.getKey(t)));
        } else if (t instanceof AlertPublishEvent) {
            filter.append("alertId", new BsonString(MetadataUtils.getKey(t)));
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
        return removeObject(collection, "name", name);
    }

    private <T> OpResult removeObject(MongoCollection<Document> collection, String nameField, String name) {
        BsonDocument filter = new BsonDocument();
        filter.append(nameField, new BsonString(name));
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
        return removeObject(schema, "streamId", streamId);
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
    public List<AlertPublishEvent> listAlertPublishEvent(int size) {
        List<AlertPublishEvent> result = list(alerts, AlertPublishEvent.class);
        if (size < 0 || size > result.size()) {
            size = result.size();
        }
        return result.subList(result.size() - size, result.size());
    }

    @Override
    public AlertPublishEvent getAlertPublishEvent(String alertId) {
        List<AlertPublishEvent> results = list(alerts, AlertPublishEvent.class);
        Optional<AlertPublishEvent> op = results.stream().filter(alert -> alert.getAlertId().equals(alertId)).findAny();
        if (op.isPresent()) {
            return op.get();
        }
        return null;
    }

    @Override
    public List<AlertPublishEvent> getAlertPublishEventByPolicyId(String policyId, int size) {
        List<AlertPublishEvent> events = list(alerts, AlertPublishEvent.class);
        List<AlertPublishEvent> result = events.stream().filter(alert -> alert.getPolicyId().equals(policyId)).collect(Collectors.toList());
        if (size < 0 || size > result.size()) {
            size = result.size();
        }
        return events.subList(result.size() - size, result.size());
    }

    @Override
    public OpResult addAlertPublishEvent(AlertPublishEvent event) {
        return addOrReplace(alerts, event);
    }

    private <T> OpResult addOne(MongoCollection<Document> collection, T t) {
        OpResult result = new OpResult();
        String json = "";
        try {
            json = mapper.writeValueAsString(t);
            collection.insertOne(Document.parse(json));
            result.code = 200;
            result.message = String.format("add one document [%s] to collection [%s] succeed!", json, collection.getNamespace());
            LOG.info(result.message);
        } catch (Exception e) {
            result.code = 400;
            result.message = e.getMessage();
            LOG.error(String.format("Add one document [%s] to collection [%s] failed!", json, collection.getNamespace()), e);
        }
        return result;
    }

    /**
     * Due to some field name in SpoutSpec contains dot(.) which is invalid Mongo Field name, we need to transform the
     * format to store in Mongo.
     * @return opresult
     */
    private <T> OpResult addOneSpoutSpec(T t) {
        OpResult result = new OpResult();
        String json = "";
        try {
            json = mapper.writeValueAsString(t);
            Document doc = Document.parse(json);

            String [] metadataMapArrays = {"kafka2TupleMetadataMap", "tuple2StreamMetadataMap", "streamRepartitionMetadataMap"};
            for (String metadataMapName: metadataMapArrays) {
                Document _metadataMapDoc = (Document) doc.get(metadataMapName);
                doc.remove(metadataMapName);

                ArrayList<Document> _metadataMapArray = new ArrayList<>();

                for ( String key : _metadataMapDoc.keySet()) {
                    Document _subDoc = new Document();
                    _subDoc.put("topicName", key);
                    _subDoc.put(metadataMapName, _metadataMapDoc.get(key));
                    _metadataMapArray.add(_subDoc);
                }
                doc.append(metadataMapName, _metadataMapArray);
            }

            spoutSpecs.insertOne(doc);
            result.code = 200;
            result.message = String.format("add one document [%s] to collection [%s] succeed!", doc.toJson(), spoutSpecs.getNamespace());
            LOG.info(result.message);
        } catch (Exception e) {
            result.code = 400;
            result.message = e.getMessage();
            LOG.error(String.format("Add one document [%s] to collection [%s] failed!", json, spoutSpecs.getNamespace()), e);
        }
        return result;
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

        if (state != null) {
            // based on version, to add content from collections of spoutSpecs/alertSpecs/etc..
            state = addDetailForScheduleState(state, versionId);
        }

        return state;
    }

    /**
     * get the basic ScheduleState, and then based on the version to get all sub-part(spoutSpecs/alertSpecs/etc)
     * to form a completed ScheduleState.
     * @return the latest ScheduleState
     */
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

        if (state != null) {
            String version = state.getVersion();
            // based on version, to add content from collections of spoutSpecs/alertSpecs/etc..
            state = addDetailForScheduleState(state, version);
        }

        return state;
    }

    @Override
    public List<ScheduleState> listScheduleStates() {
        throw new UnsupportedOperationException("listScheduleStates not support!");
    }

    @Override
    public OpResult clearScheduleState(int maxCapacity) {
        throw new UnsupportedOperationException("clearScheduleState not support!");
    }

    private ScheduleState addDetailForScheduleState(ScheduleState state, String version) {
        Map<String, SpoutSpec> spoutMaps = maps(spoutSpecs, SpoutSpec.class, version);
        if (spoutMaps.size() != 0) {
            state.setSpoutSpecs(spoutMaps);
        }

        Map<String, AlertBoltSpec> alertMaps = maps(alertSpecs, AlertBoltSpec.class, version);
        if (alertMaps.size() != 0) {
            state.setAlertSpecs(alertMaps);
        }

        Map<String, RouterSpec> groupMaps = maps(groupSpecs, RouterSpec.class, version);
        if (groupMaps.size() != 0) {
            state.setGroupSpecs(groupMaps);
        }

        Map<String, PublishSpec> publishMaps = maps(publishSpecs, PublishSpec.class, version);
        if (publishMaps.size() != 0) {
            state.setPublishSpecs(publishMaps);
        }

        List<VersionedPolicyDefinition> policyLists = list(policySnapshots, VersionedPolicyDefinition.class, version);
        if (policyLists.size() != 0) {
            state.setPolicySnapshots(policyLists);
        }

        List<VersionedStreamDefinition> streamLists = list(streamSnapshots, VersionedStreamDefinition.class, version);
        if (streamLists.size() != 0) {
            state.setStreamSnapshots(streamLists);
        }

        List<MonitoredStream> monitorLists = list(monitoredStreams, MonitoredStream.class, version);
        if (monitorLists.size() != 0) {
            state.setMonitoredStreams(monitorLists);
        }

        List<PolicyAssignment> assignmentLists = list(assignments, PolicyAssignment.class, version);
        if (assignmentLists.size() != 0) {
            state.setAssignments(assignmentLists);
        }
        return state;
    }

    private <T> Map<String, T> maps(MongoCollection<Document> collection, Class<T> clz, String version) {
        BsonDocument doc = new BsonDocument();
        doc.append("version", new BsonString(version));

        Map<String, T> maps = new HashMap<String, T>();
        String mapKey = (clz == SpoutSpec.class) ? "topologyId" : "topologyName";
        collection.find(doc).forEach(new Block<Document>() {
            @Override
            public void apply(Document document) {
                String json = document.toJson();
                try {
                    //Due to some field name in SpoutSpec contains dot(.) which is invalid Mongo Field name,
                    // we need to transform the format while reading from Mongo.
                    if (clz == SpoutSpec.class) {
                        Document doc = Document.parse(json);
                        String [] metadataMapArrays = {"kafka2TupleMetadataMap", "tuple2StreamMetadataMap", "streamRepartitionMetadataMap"};
                        for (String metadataMapName: metadataMapArrays) {
                            ArrayList<Document> subDocs = (ArrayList) doc.get(metadataMapName);
                            doc.remove(metadataMapName);

                            Document replaceDoc = new Document();
                            for ( Document subDoc : subDocs) {
                                replaceDoc.put((String) subDoc.get("topicName"), subDoc.get(metadataMapName));
                            }
                            doc.put(metadataMapName, replaceDoc);
                        }

                        json = doc.toJson();
                    }
                    T t = mapper.readValue(json, clz);
                    maps.put(document.getString(mapKey), t);
                } catch (IOException e) {
                    LOG.error("deserialize config item failed!", e);
                }
            }
        });

        return maps;
    }

    private <T> List<T> list(MongoCollection<Document> collection, Class<T> clz, String version) {
        BsonDocument doc = new BsonDocument();
        doc.append("version", new BsonString(version));

        List<T> result = new LinkedList<T>();
        collection.find(doc).map(new Function<Document, T>() {
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

    /**
     * write ScheduleState to several collections. basic info writes to ScheduleState, other writes to collections
     * named by spoutSpecs/alertSpecs/etc.
     *
     * @param state
     * @return
     */
    @Override
    public OpResult addScheduleState(ScheduleState state) {
        OpResult result = new OpResult();
        try {
            for (String key : state.getSpoutSpecs().keySet()) {
                SpoutSpec spoutSpec = state.getSpoutSpecs().get(key);
                addOneSpoutSpec(spoutSpec);
            }

            for (String key : state.getAlertSpecs().keySet()) {
                AlertBoltSpec alertBoltSpec = state.getAlertSpecs().get(key);
                addOne(alertSpecs, alertBoltSpec);
            }

            for (String key : state.getGroupSpecs().keySet()) {
                RouterSpec groupSpec = state.getGroupSpecs().get(key);
                addOne(groupSpecs, groupSpec);
            }

            for (String key : state.getPublishSpecs().keySet()) {
                PublishSpec publishSpec = state.getPublishSpecs().get(key);
                addOne(publishSpecs, publishSpec);
            }

            for (VersionedPolicyDefinition policySnapshot : state.getPolicySnapshots()) {
                addOne(policySnapshots, policySnapshot);
            }

            for (VersionedStreamDefinition streamSnapshot : state.getStreamSnapshots()) {
                addOne(streamSnapshots, streamSnapshot);
            }

            for (MonitoredStream monitoredStream : state.getMonitoredStreams()) {
                addOne(monitoredStreams, monitoredStream);
            }

            for (PolicyAssignment assignment : state.getAssignments()) {
                addOne(assignments, assignment);
            }

            ScheduleStateBase stateBase = new ScheduleStateBase(
                    state.getVersion(), state.getGenerateTime(), state.getCode(),
                    state.getMessage(), state.getScheduleTimeMillis());

            addOne(scheduleStates, stateBase);

            result.code = 200;
            result.message = "add document to collection schedule_specs succeed";
        } catch (Exception e) {
            result.code = 400;
            result.message = e.getMessage();
            LOG.error("", e);
        }
        return result;
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
