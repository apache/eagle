/*
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

package org.apache.eagle.app;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.AbstractSchedulingPlan;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.SchedulingPlan;
import org.apache.eagle.app.job.plugins.XMLStreamSchedulingDataProcessor;
import org.quartz.SchedulerException;
import org.quartz.spi.MutableTrigger;
import org.quartz.xml.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

public class XmlSchedulingApplication extends SchedulingApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlSchedulingPlan.class);

    private final String schedulingXmlFile;

    public XmlSchedulingApplication(String schedulingXmlFile) {
        this.schedulingXmlFile = schedulingXmlFile;
    }

    @Override
    public SchedulingPlan execute(Config config, ScheduledEnvironment environment) {
        return new XmlSchedulingPlan(schedulingXmlFile, config, environment);
    }

    private class XmlSchedulingPlan extends AbstractSchedulingPlan {
        private final String schedulingXmlFile;

        public XmlSchedulingPlan(String schedulingXmlFile, Config config, ScheduledEnvironment environment) {
            super(config, environment);
            this.schedulingXmlFile = schedulingXmlFile;
        }

        private InputStream getSchedulingXmlAsStream() {
            String metaInfoSchedulingXmlFile = "/META-INF/jobs/" + this.schedulingXmlFile;

            InputStream inputStream = XmlSchedulingPlan.class.getResourceAsStream("/META-INF/jobs/" + this.schedulingXmlFile);
            if (inputStream == null) {
                inputStream = XmlSchedulingPlan.class.getResourceAsStream(this.schedulingXmlFile);
            }
            Preconditions.checkNotNull(inputStream, "Unable to load resource " + metaInfoSchedulingXmlFile);
            return inputStream;
        }

        @Override
        public void schedule() throws SchedulerException {
            try {
                XMLStreamSchedulingDataProcessor processor = new XMLStreamSchedulingDataProcessor();
                processor.processStreamWithConfigAndScheduleJobs(
                    getSchedulingXmlAsStream(),
                    this.getConfig(),
                    getAppId(),
                    getScheduler()
                );
            } catch (ParserConfigurationException | IOException | XPathException | ParseException | SAXException | ValidationException | ClassNotFoundException e) {
                LOGGER.error("Failed to schedule {}: {}", this.getAppId(), e.getMessage(), e);
                throw new SchedulerException(e.getMessage(), e);
            }
        }

        @Override
        public boolean unschedule() throws SchedulerException {
            try {
                XMLStreamSchedulingDataProcessor processor = new XMLStreamSchedulingDataProcessor();
                processor.processStreamWithConfigAndUnscheduleJobs(
                    getSchedulingXmlAsStream(),
                    getConfig(),
                    getAppId(),
                    getScheduler()
                );
                return true;
            } catch (ParserConfigurationException | IOException | XPathException | ParseException | SAXException | ValidationException | ClassNotFoundException e) {
                LOGGER.error("Failed to unschedule {}: {}", this.getAppId(), e.getMessage(), e);
                throw new SchedulerException(e.getMessage(), e);
            }
        }

        @Override
        public boolean scheduling() throws SchedulerException {
            try {
                XMLStreamSchedulingDataProcessor processor = new XMLStreamSchedulingDataProcessor();
                processor.processStreamWithConfig(
                    getSchedulingXmlAsStream(),
                    getConfig(),
                    getAppId()
                );
                boolean scheduling = false;
                for (MutableTrigger trigger : processor.getParsedTriggers()) {
                    if (this.getScheduler().checkExists(trigger.getKey())) {
                        LOGGER.debug("Job {} is scheduling with trigger {}", trigger.getJobKey(), trigger.getKey());
                        scheduling = true;
                    } else {
                        LOGGER.debug("Trigger {} not exists", trigger.getJobKey(), trigger.getKey());
                    }
                }
                return scheduling;
            } catch (ParserConfigurationException | IOException | XPathException | ParseException | SAXException | ValidationException | ClassNotFoundException e) {
                LOGGER.error("Failed to check {} scheduling status: {}", this.getAppId(), e.getMessage(), e);
                throw new SchedulerException(e.getMessage(), e);
            }
        }
    }
}