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

package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.engine.spark.model.PublishState;
import org.apache.eagle.alert.engine.utils.Constants;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class AlertPublisherBoltFunction implements VoidFunction<Iterator<Tuple2<String, AlertStreamEvent>>> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertPublisherBoltFunction.class);
    private String alertPublishBoltName = "alertPublishBolt";
    private AtomicReference<PublishSpec> publishSpecRef;
    private PublishState publishState;
    private Map<String, Publishment> cachedPublishments = new HashMap<>();


    public AlertPublisherBoltFunction(AtomicReference<PublishSpec> publishSpecRef, String alertPublishBoltName, PublishState publishState) {
        this.alertPublishBoltName = alertPublishBoltName;
        this.publishSpecRef = publishSpecRef;
        this.publishState = publishState;
    }

    @Override
    public void call(Iterator<Tuple2<String, AlertStreamEvent>> tuple2Iterator) throws Exception {
        PublishSpec publishSpec;
        AlertPublisherImpl alertPublisher = null;
        String streamId = Constants.UNKNOW_STREAMID;
        while (tuple2Iterator.hasNext()) {
            Tuple2<String, AlertStreamEvent> tuple2 = tuple2Iterator.next();
            if (streamId == Constants.UNKNOW_STREAMID) {
                streamId = tuple2._1;
            }
            if (alertPublisher == null) {
                cachedPublishments = publishState.getCachedPublishmentsByStreamId(streamId);
                alertPublisher = new AlertPublisherImpl(alertPublishBoltName, new ArrayList<>(cachedPublishments.values()));
                alertPublisher.init(null, new HashMap<>());
                publishSpec = publishSpecRef.get();

                onAlertPublishSpecChange(alertPublisher, publishSpec, cachedPublishments);
                publishState.store(streamId, cachedPublishments);
            }
            AlertStreamEvent alertEvent = tuple2._2;
            alertPublisher.nextEvent(alertEvent);
        }
        if (alertPublisher != null) {
            alertPublisher.close();
        }
    }

    private void onAlertPublishSpecChange(AlertPublisher alertPublisher, PublishSpec pubSpec, final Map<String, Publishment> cachedPublishments) {
        if (pubSpec == null) {
            return;
        }

        List<Publishment> newPublishments = pubSpec.getPublishments();
        if (newPublishments == null) {
            LOG.info("no publishments with PublishSpec {} for this topology", pubSpec);
            return;
        }
        Map<String, Publishment> newPublishmentsMap = new HashMap<>();
        newPublishments.forEach(p -> newPublishmentsMap.put(p.getName(), p));
        MapComparator<String, Publishment> comparator = new MapComparator<>(newPublishmentsMap, cachedPublishments);
        comparator.compare();
        List<Publishment> beforeModified = new ArrayList<>();
        comparator.getModified().forEach(p -> beforeModified.add(cachedPublishments.get(p.getName())));
        alertPublisher.onPublishChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), beforeModified);
        this.cachedPublishments = newPublishmentsMap;
    }
}
