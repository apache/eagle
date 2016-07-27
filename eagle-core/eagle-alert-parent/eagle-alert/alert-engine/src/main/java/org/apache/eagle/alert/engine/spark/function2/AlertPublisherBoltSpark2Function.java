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
package org.apache.eagle.alert.engine.spark.function2;


import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class AlertPublisherBoltSpark2Function implements MapPartitionsFunction<Tuple2<String, AlertStreamEvent>, Tuple2<String, String>> {

    private final static Logger LOG = LoggerFactory.getLogger(AlertPublisherBoltSpark2Function.class);

    private Map<String, Publishment> cachedPublishments = new HashMap<>();

    private PublishSpec pubSpec;
    private Map<String, StreamDefinition> sds;
    private String alertPublishBoltName = "alertPublishBolt";
    private Config config;


    public AlertPublisherBoltSpark2Function(Config config, String alertPublishBoltName) {
        this.alertPublishBoltName = alertPublishBoltName;
        this.config = config;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String, AlertStreamEvent>> tuple2Iterator) throws Exception {

        AlertPublisher alertPublisher = new AlertPublisherImpl(alertPublishBoltName);
        alertPublisher.init(null, new HashMap<>());
        SpecMetadataServiceClientImpl client = new SpecMetadataServiceClientImpl(config);
        pubSpec = client.getPublishSpec();
        sds = client.getSds();
        onAlertPublishSpecChange(alertPublisher, pubSpec, sds);
        List<Tuple2<String, String>> result = new ArrayList<>();
        while (tuple2Iterator.hasNext()) {
            Tuple2<String, AlertStreamEvent> tuple2 = tuple2Iterator.next();
            AlertStreamEvent alertEvent = tuple2._2;
            alertPublisher.nextEvent((alertEvent));
            LOG.info("executed " + alertEvent.getPolicyId());
            result.add(new Tuple2<>("executed",alertEvent.getPolicyId()));
        }

        return result.iterator();
    }

    private void onAlertPublishSpecChange(AlertPublisher alertPublisher, PublishSpec pubSpec, Map<String, StreamDefinition> sds) {
        if (pubSpec == null) return;

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

        // switch
        cachedPublishments = newPublishmentsMap;
    }
}
