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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.bson.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class TransformerUtils {

    public static final String MAP_KEY = "key";
    public static final String MAP_VALUE = "value";

    @SuppressWarnings("unchecked")
    public static <T> T transform(Class<T> klass, BsonDocument doc) {
        if (klass.equals(DedupEntity.class)) {
            String streamId = doc.getString(MongoDedupEventsStore.DEDUP_STREAM_ID).getValue();
            String policyId = doc.getString(MongoDedupEventsStore.DEDUP_POLICY_ID).getValue();
            long timestamp = doc.getInt64(MongoDedupEventsStore.DEDUP_TIMESTAMP).getValue();
            HashMap<String, String> customFieldValues = new HashMap<String, String>();
            BsonArray customFieldsValuesArray = doc.getArray(
                MongoDedupEventsStore.DEDUP_CUSTOM_FIELDS_VALUES);
            for (int i = 0; i < customFieldsValuesArray.size(); i++) {
                BsonDocument dedupCustomFieldValuesDoc = customFieldsValuesArray.get(i).asDocument();
                customFieldValues.put(
                    dedupCustomFieldValuesDoc.getString(MAP_KEY).getValue(),
                    dedupCustomFieldValuesDoc.getString(MAP_VALUE).getValue());
            }
            EventUniq eventUniq = new EventUniq(streamId, policyId, timestamp, customFieldValues);
            eventUniq.removable = doc.getBoolean(MongoDedupEventsStore.DEDUP_REMOVABLE).getValue();
            eventUniq.createdTime = doc.getInt64(
                MongoDedupEventsStore.DEDUP_CREATE_TIME, new BsonInt64(0)).getValue();
            List<DedupValue> dedupValues = new ArrayList<DedupValue>();
            BsonArray dedupValuesArray = doc.getArray(MongoDedupEventsStore.DEDUP_VALUES);
            for (int i = 0; i < dedupValuesArray.size(); i++) {
                BsonDocument dedupValuesDoc = dedupValuesArray.get(i).asDocument();
                DedupValue dedupValue = new DedupValue();
                dedupValue.setStateFieldValue(dedupValuesDoc.getString(
                    MongoDedupEventsStore.DEDUP_STATE_FIELD_VALUE).getValue());
                dedupValue.setCount(dedupValuesDoc.getInt64(
                    MongoDedupEventsStore.DEDUP_COUNT).getValue());
                dedupValue.setFirstOccurrence(dedupValuesDoc.getInt64(
                    MongoDedupEventsStore.DEDUP_FIRST_OCCURRENCE).getValue());
                dedupValue.setCloseTime(dedupValuesDoc.getInt64(
                    MongoDedupEventsStore.DEDUP_CLOSE_TIME).getValue());
                dedupValue.setDocId(dedupValuesDoc.getString(
                    MongoDedupEventsStore.DOC_ID).getValue());
                dedupValues.add(dedupValue);
            }
            String publishId = doc.getString(MongoDedupEventsStore.DEDUP_PUBLISH_ID).getValue();
            return (T) new DedupEntity(publishId, eventUniq, dedupValues);
        }
        throw new RuntimeException(String.format("Unknow object type %s, cannot transform", klass.getName()));
    }

    public static BsonDocument transform(Object obj) {
        if (obj instanceof DedupEntity) {
            BsonDocument doc = new BsonDocument();
            DedupEntity entity = (DedupEntity) obj;
            doc.put(MongoDedupEventsStore.DEDUP_ID, new BsonInt64(getUniqueId(entity.getPublishName(), entity.getEventEniq())));
            doc.put(MongoDedupEventsStore.DEDUP_STREAM_ID, new BsonString(entity.getEventEniq().streamId));
            doc.put(MongoDedupEventsStore.DEDUP_PUBLISH_ID, new BsonString(entity.getPublishName()));
            doc.put(MongoDedupEventsStore.DEDUP_POLICY_ID, new BsonString(entity.getEventEniq().policyId));
            doc.put(MongoDedupEventsStore.DEDUP_CREATE_TIME, new BsonInt64(entity.getEventEniq().createdTime));
            doc.put(MongoDedupEventsStore.DEDUP_TIMESTAMP, new BsonInt64(entity.getEventEniq().timestamp));
            doc.put(MongoDedupEventsStore.DEDUP_REMOVABLE, new BsonBoolean(entity.getEventEniq().removable));

            List<BsonDocument> dedupCustomFieldValues = new ArrayList<BsonDocument>();
            for (Entry<String, String> entry : entity.getEventEniq().customFieldValues.entrySet()) {
                BsonDocument dedupCustomFieldValuesDoc = new BsonDocument();
                dedupCustomFieldValuesDoc.put(MAP_KEY, new BsonString(entry.getKey()));
                dedupCustomFieldValuesDoc.put(MAP_VALUE, new BsonString(entry.getValue()));
                dedupCustomFieldValues.add(dedupCustomFieldValuesDoc);
            }
            doc.put(MongoDedupEventsStore.DEDUP_CUSTOM_FIELDS_VALUES, new BsonArray(dedupCustomFieldValues));

            List<BsonDocument> dedupValuesDocs = new ArrayList<BsonDocument>();
            for (DedupValue dedupValue : entity.getDedupValues()) {
                BsonDocument dedupValuesDoc = new BsonDocument();
                dedupValuesDoc.put(MongoDedupEventsStore.DEDUP_STATE_FIELD_VALUE, new BsonString(dedupValue.getStateFieldValue()));
                dedupValuesDoc.put(MongoDedupEventsStore.DEDUP_COUNT, new BsonInt64(dedupValue.getCount()));
                dedupValuesDoc.put(MongoDedupEventsStore.DEDUP_FIRST_OCCURRENCE,new BsonInt64(dedupValue.getFirstOccurrence()));
                dedupValuesDoc.put(MongoDedupEventsStore.DEDUP_CLOSE_TIME, new BsonInt64(dedupValue.getCloseTime()));
                dedupValuesDoc.put(MongoDedupEventsStore.DOC_ID, new BsonString(dedupValue.getDocId()));
                dedupValuesDocs.add(dedupValuesDoc);
            }
            doc.put(MongoDedupEventsStore.DEDUP_VALUES, new BsonArray(dedupValuesDocs));
            return doc;
        }
        throw new RuntimeException(String.format("Unknow object type %s, cannot transform", obj.getClass().getName()));
    }

    public static int getUniqueId(String publishName, EventUniq eventEniq) {
        HashCodeBuilder builder = new HashCodeBuilder().append(eventEniq).append(publishName);
        return builder.build();
    }

}
