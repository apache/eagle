/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.job;

import org.junit.Assert;
import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.ThreadContextClassLoadHelper;
import org.quartz.xml.XMLSchedulingDataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLSchedulingDataProcessorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLSchedulingDataProcessorTest.class);

    @Test
    public void testXMLScheduling() {
        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            XMLSchedulingDataProcessor e = new XMLSchedulingDataProcessor(new ThreadContextClassLoadHelper());
            e.addJobGroupToNeverDelete("XMLSchedulingDataProcessorTest");
            e.addTriggerGroupToNeverDelete("XMLSchedulingDataProcessorTest");
            e.processStreamAndScheduleJobs(XMLSchedulingDataProcessorTest.class.getResourceAsStream("/TestJobScheduling.xml"), "XMLFileInputStream", scheduler);
            scheduler.shutdown(true);
        } catch (Exception e) {
            LOGGER.error("Error scheduling jobs: " + e.getMessage(), e);
            Assert.fail("Error scheduling jobs: " + e.getMessage());
        }
    }
}
