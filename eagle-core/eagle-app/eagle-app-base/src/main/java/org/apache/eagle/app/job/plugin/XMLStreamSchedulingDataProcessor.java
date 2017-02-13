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
package org.apache.eagle.app.job.plugin;

import com.typesafe.config.Config;
import org.apache.eagle.common.config.ConfigStringResolver;
import org.apache.velocity.app.VelocityEngine;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.simpl.ThreadContextClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.MutableTrigger;
import org.quartz.xml.ValidationException;
import org.quartz.xml.XMLSchedulingDataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

public class XMLStreamSchedulingDataProcessor extends XMLSchedulingDataProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLStreamSchedulingDataProcessor.class);

    public XMLStreamSchedulingDataProcessor(ClassLoadHelper classLoadHelper) throws ParserConfigurationException {
        super(classLoadHelper);
        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
    }

    public XMLStreamSchedulingDataProcessor() throws ParserConfigurationException {
        super(new ThreadContextClassLoadHelper());
    }

    public void processStreamAndUnscheduleJobs(InputStream stream, String systemId, Scheduler scheduler)
        throws SchedulerException, ValidationException, ClassNotFoundException, SAXException, XPathException, ParseException, IOException {
        this.prepForProcessing();
        LOGGER.info("Parsing XML from stream with systemId: " + systemId);
        InputSource is = new InputSource(stream);
        is.setSystemId(systemId);
        this.process(is);
        this.maybeThrowValidationException();
        this.getLoadedJobs();
        List<TriggerKey> triggerKeyList = new LinkedList<>();
        for (MutableTrigger trigger : this.getLoadedTriggers()) {
            triggerKeyList.add(trigger.getKey());
        }
        scheduler.unscheduleJobs(triggerKeyList);
        LOGGER.info("Unscheduled {} triggers: {}", triggerKeyList.size(), triggerKeyList);
    }


    public void processStreamWithConfigAndScheduleJobs(InputStream stream, Config config, String systemId, Scheduler scheduler)
        throws SchedulerException, ValidationException, ClassNotFoundException, SAXException, XPathException, ParseException, IOException, ParserConfigurationException {
        processStreamAndScheduleJobs(new ConfigStringResolver(config).resolveAsStream(stream), systemId, scheduler);
    }

    public void processStreamWithConfigAndUnscheduleJobs(InputStream stream, Config config, String systemId, Scheduler scheduler)
        throws SchedulerException, ValidationException, ClassNotFoundException, SAXException, XPathException, ParseException, IOException {
        processStreamAndUnscheduleJobs(new ConfigStringResolver(config).resolveAsStream(stream), systemId, scheduler);
    }

    public void processStreamWithConfig(InputStream stream, Config config, String systemId)
        throws SchedulerException, ValidationException, ClassNotFoundException, SAXException, XPathException, ParseException, IOException {
        this.prepForProcessing();
        LOGGER.info("Parsing XML from stream with systemId: " + systemId);
        InputSource is = new InputSource(new ConfigStringResolver(config).resolveAsStream(stream));
        is.setSystemId(systemId);
        this.process(is);
        this.maybeThrowValidationException();
    }

    public List<MutableTrigger> getParsedTriggers() {
        return this.getLoadedTriggers();
    }

    public List<JobDetail> getParsedJobs() {
        return this.getLoadedJobs();
    }
}