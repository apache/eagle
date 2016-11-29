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

package org.apache.eagle.alert.coordinator.trigger;

import com.google.common.base.Stopwatch;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ScheduleStateCleaner implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(ScheduleStateCleaner.class);

    private IMetadataServiceClient client;
    private int reservedCapacity;

    public ScheduleStateCleaner(IMetadataServiceClient client, int capacity) {
        this.client = client;
        this.reservedCapacity = capacity;
    }

    @Override
    public void run() {
        // we should catch every exception to avoid zombile thread
        try {
            final Stopwatch watch = Stopwatch.createStarted();
            LOG.info("clear schedule states start.");
            client.clearScheduleState(reservedCapacity);
            watch.stop();
            LOG.info("clear schedule states completed. used time milliseconds: {}", watch.elapsed(TimeUnit.MILLISECONDS));
            // reset cached policies
        } catch (Throwable t) {
            LOG.error("fail to clear schedule states due to {}, but continue to run", t.getMessage());
        }
    }
}
