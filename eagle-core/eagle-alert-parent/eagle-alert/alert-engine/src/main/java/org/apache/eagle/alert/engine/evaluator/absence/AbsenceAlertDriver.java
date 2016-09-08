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
package org.apache.eagle.alert.engine.evaluator.absence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * this assumes that event comes in time order.
 * Since 7/7/16.
 */
public class AbsenceAlertDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AbsenceAlertDriver.class);
    private List<Object> expectedAttrs;
    private AbsenceWindowProcessor processor;
    private AbsenceWindowGenerator windowGenerator;

    public AbsenceAlertDriver(List<Object> expectedAttrs, AbsenceWindowGenerator windowGenerator) {
        this.expectedAttrs = expectedAttrs;
        this.windowGenerator = windowGenerator;
    }

    public boolean process(List<Object> appearAttrs, long occurTime) {
        // initialize window
        if (processor == null) {
            processor = nextProcessor(occurTime);
            LOG.info("initialized a new window {}", processor);
        }
        processor.process(appearAttrs, occurTime);
        AbsenceWindowProcessor.OccurStatus status = processor.checkStatus();
        boolean expired = processor.checkExpired();
        boolean isAbsenceAlert = false;
        if (expired) {
            if (status == AbsenceWindowProcessor.OccurStatus.absent) {
                // send alert
                LOG.info("===================");
                LOG.info("|| Absence Alert ||");
                LOG.info("===================");
                isAbsenceAlert = true;
                // figure out next window and set the new window
            }
            processor = nextProcessor(occurTime);
            LOG.info("created a new window {}", processor);
        }

        return isAbsenceAlert;
    }

    /**
     * calculate absolute time range based on current timestamp.
     *
     * @param currTime milliseconds
     * @return
     */
    private AbsenceWindowProcessor nextProcessor(long currTime) {
        AbsenceWindow window = windowGenerator.nextWindow(currTime);
        return new AbsenceWindowProcessor(expectedAttrs, window);
    }
}