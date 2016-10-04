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
package org.apache.eagle.alert.engine.publisher.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.bson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MongoDedupEventsStore implements DedupEventsStore {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDedupEventsStore.class);

    public static final String DEDUP_ID = "dedupId";
    public static final String DEDUP_STREAM_ID = "streamId";
    public static final String DOC_ID = "docId";
    public static final String DEDUP_POLICY_ID = "policyId";
    public static final String DEDUP_CREATE_TIME = "createdTime";
    public static final String DEDUP_TIMESTAMP = "timestamp";
    public static final String DEDUP_REMOVABLE = "removable";
    public static final String DEDUP_CUSTOM_FIELDS_VALUES = "customFieldValues";
    public static final String DEDUP_VALUES = "dedupValues";
    public static final String DEDUP_STATE_FIELD_VALUE = "stateFieldValue";
    public static final String DEDUP_COUNT = "count";
    public static final String DEDUP_FIRST_OCCURRENCE = "firstOccurrence";
    public static final String DEDUP_CLOSE_TIME = "closeTime";
    public static final String DEDUP_PUBLISH_ID = "publishId";

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private Config config;
    private String connection;
    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> stateCollection;
    private String publishName;

    private static final String DB_NAME = "ump_alert_dedup";
    private static final String ALERT_STATE_COLLECTION = "alert_dedup";

    public MongoDedupEventsStore(Config config, String publishName) {
        this.config = config;
        this.publishName = publishName;
        this.connection = this.config.getString("connection");
        try {
            this.client = new MongoClient(new MongoClientURI(this.connection));
            init();
        } catch (Throwable t) {
            LOG.error(String.format("initialize mongodb %s client failed", this.connection), t);
        }
    }

    private void init() {
        db = client.getDatabase(DB_NAME);
        stateCollection = db.getCollection(ALERT_STATE_COLLECTION);
        // dedup id index
        IndexOptions io = new IndexOptions().background(true).unique(true).name(DEDUP_ID + "_index");
        BsonDocument doc = new BsonDocument();
        doc.append(DEDUP_ID, new BsonInt32(1));
        stateCollection.createIndex(doc, io);
    }

    @Override
    public Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> getEvents() {
        try {
            Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> result = new ConcurrentHashMap<EventUniq, ConcurrentLinkedDeque<DedupValue>>();
            BsonDocument filter = new BsonDocument();
            filter.append(DEDUP_PUBLISH_ID, new BsonString(this.publishName));
            stateCollection.find(filter).forEach(new Block<Document>() {
                @Override
                public void apply(final Document doc) {
                    DedupEntity entity = TransformerUtils.transform(DedupEntity.class, BsonDocument.parse(doc.toJson()));
                    result.put(entity.getEventEniq(), entity.getDedupValuesInConcurrentLinkedDeque());
                }
            });
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found {} dedup events from mongoDB", result.size());
            }
            return result;
        } catch (Exception e) {
            LOG.error("find dedup state failed, but the state in memory is good, could be ingored.", e);
        }
        return new HashMap<EventUniq, ConcurrentLinkedDeque<DedupValue>>();
    }

    @Override
    public void add(EventUniq eventEniq, ConcurrentLinkedDeque<DedupValue> dedupStateValues) {
        try {
            BsonDocument doc = TransformerUtils.transform(new DedupEntity(this.publishName, eventEniq, dedupStateValues));
            BsonDocument filter = new BsonDocument();
            filter.append(DEDUP_ID, new BsonInt64(TransformerUtils.getUniqueId(this.publishName, eventEniq)));
            Document returnedDoc = stateCollection.findOneAndReplace(filter, Document.parse(doc.toJson()));
            if (returnedDoc == null) {
                InsertOneOptions option = new InsertOneOptions();
                stateCollection.insertOne(Document.parse(doc.toJson()), option);
            }
        } catch (Exception e) {
            LOG.error("insert dedup state failed, but the state is still in memory, could be ingored.", e);
        }
    }

    @Override
    public void remove(EventUniq eventEniq) {
        try {
            BsonDocument filter = new BsonDocument();
            filter.append(DEDUP_ID, new BsonInt64(TransformerUtils.getUniqueId(this.publishName, eventEniq)));
            stateCollection.deleteOne(filter);
        } catch (Exception e) {
            LOG.error("delete dedup state failed, but the state in memory is good, could be ingored.", e);
        }
    }

}
