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

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.utils.Constants;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AlertPublisherBoltFunction implements VoidFunction<Iterator<Tuple2<String, AlertStreamEvent>>> {
    private static final Logger LOG = LoggerFactory.getLogger(AlertPublisherBoltFunction.class);


    private String alertPublishBoltName = "alertPublishBolt";
    private AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef;

    public AlertPublisherBoltFunction(String alertPublishBoltName, AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef) {
        this.alertPublishBoltName = alertPublishBoltName;
        this.publishmentsChangInfoRef = publishmentsChangInfoRef;
    }

    @Override
    public void call(Iterator<Tuple2<String, AlertStreamEvent>> tuple2Iterator) throws Exception {
        AlertPublisher alertPublisher = new AlertPublisherImpl(alertPublishBoltName);
        alertPublisher.init(null, new HashMap<>());
        Map<String, List<Publishment>> publishmentsChangInfo = publishmentsChangInfoRef.get();
        alertPublisher.onPublishChange(publishmentsChangInfo.get(Constants.ADDED), publishmentsChangInfo.get(Constants.REMOVED), publishmentsChangInfo.get(Constants.MODIFIED), publishmentsChangInfo.get(Constants.BEFORE_MODIFIED));
        while (tuple2Iterator.hasNext()) {
            Tuple2<String, AlertStreamEvent> tuple2 = tuple2Iterator.next();
            AlertStreamEvent alertEvent = tuple2._2;
            LOG.info("AlertPublisherBoltFunction " + alertEvent);
            alertPublisher.nextEvent(alertEvent);
        }
    }
}
